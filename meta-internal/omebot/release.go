//go:build meta_internal

package omebot

import (
	"context"
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

var (
	stagesGVR  = schema.GroupVersionResource{Group: "kargo.akuity.io", Version: "v1alpha1", Resource: "stages"}
	freightGVR = schema.GroupVersionResource{Group: "kargo.akuity.io", Version: "v1alpha1", Resource: "freights"}
)

// Release reads the release pipeline state from one Kargo project.
type Release struct {
	Client    dynamic.Interface
	Namespace string
	// Recent is how many of the newest Freight objects to report.
	Recent int
}

// NewRelease builds the release reader from the kargo config, or returns nil
// when no Kargo project is configured. An inCluster reader authenticates as
// the pod's ServiceAccount.
func NewRelease(cfg *Config, prober *Prober) (*Release, error) {
	if cfg.Kargo == nil {
		return nil, nil
	}
	cl := Cluster{Name: "in-cluster", InCluster: true}
	if !cfg.Kargo.InCluster {
		var ok bool
		if cl, ok = cfg.Cluster(cfg.Kargo.Cluster); !ok {
			return nil, fmt.Errorf("kargo cluster %q is not registered", cfg.Kargo.Cluster)
		}
	}
	rc, err := prober.restConfig(cl)
	if err != nil {
		return nil, fmt.Errorf("kargo cluster %s: %w", cl.Name, err)
	}
	client, err := dynamic.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("kargo client: %w", err)
	}
	return &Release{Client: client, Namespace: cfg.Kargo.Namespace, Recent: cfg.Kargo.RecentFreight}, nil
}

// StageRow is one Kargo Stage. Absent fields are "-".
type StageRow struct {
	Name           string `json:"name"`
	Freight        string `json:"freight"`
	Charts         string `json:"charts"`
	Health         string `json:"health"`
	LastPromotion  string `json:"lastPromotion"`
	PromotionPhase string `json:"promotionPhase"`
	FinishedAt     string `json:"finishedAt"`
}

// FreightRow is one Freight object, newest first.
type FreightRow struct {
	Name       string   `json:"name"`
	Alias      string   `json:"alias"`
	Created    string   `json:"created"`
	Charts     string   `json:"charts"`
	VerifiedIn []string `json:"verifiedIn,omitempty"`
}

// ReleaseReport is the body of GET /v1/release.
type ReleaseReport struct {
	Stages   []StageRow      `json:"stages"`
	Freight  []FreightRow    `json:"freight"`
	Clusters []ClusterStatus `json:"clusters"`
}

// Report lists Stages and recent Freight and attaches the running version
// per registry cluster.
func (r *Release) Report(ctx context.Context, clusters []ClusterStatus) (ReleaseReport, error) {
	rep := ReleaseReport{Clusters: clusters}
	stages, err := r.Client.Resource(stagesGVR).Namespace(r.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return rep, fmt.Errorf("list stages: %w", err)
	}
	for _, s := range stages.Items {
		rep.Stages = append(rep.Stages, stageRow(s))
	}
	sort.Slice(rep.Stages, func(i, j int) bool { return rep.Stages[i].Name < rep.Stages[j].Name })

	freight, err := r.Client.Resource(freightGVR).Namespace(r.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return rep, fmt.Errorf("list freight: %w", err)
	}
	items := freight.Items
	sort.Slice(items, func(i, j int) bool {
		ti, tj := items[i].GetCreationTimestamp(), items[j].GetCreationTimestamp()
		return tj.Before(&ti)
	})
	for i, f := range items {
		if i >= r.Recent {
			break
		}
		rep.Freight = append(rep.Freight, freightRow(f))
	}
	return rep, nil
}

func stageRow(s unstructured.Unstructured) StageRow {
	row := StageRow{Name: s.GetName(), Freight: "-", Charts: "-"}
	history, _, _ := unstructured.NestedSlice(s.Object, "status", "freightHistory")
	if len(history) > 0 {
		if current, ok := history[0].(map[string]any); ok {
			items, _, _ := unstructured.NestedMap(current, "items")
			var names, charts []string
			for _, key := range sortedKeys(items) {
				ref, _ := items[key].(map[string]any)
				if n, _, _ := unstructured.NestedString(ref, "name"); n != "" {
					names = append(names, n)
				}
				list, _, _ := unstructured.NestedSlice(ref, "charts")
				charts = append(charts, chartVersions(list)...)
			}
			row.Freight = joinOrDash(names)
			row.Charts = joinOrDash(charts)
		}
	}
	row.Health = nestedOrDash(s.Object, "status", "health", "status")
	row.LastPromotion = nestedOrDash(s.Object, "status", "lastPromotion", "name")
	row.PromotionPhase = nestedOrDash(s.Object, "status", "lastPromotion", "status", "phase")
	row.FinishedAt = nestedOrDash(s.Object, "status", "lastPromotion", "finishedAt")
	return row
}

func freightRow(f unstructured.Unstructured) FreightRow {
	charts, _, _ := unstructured.NestedSlice(f.Object, "charts")
	verified, _, _ := unstructured.NestedMap(f.Object, "status", "verifiedIn")
	created := f.GetCreationTimestamp()
	return FreightRow{
		Name:       f.GetName(),
		Alias:      nestedOrDash(f.Object, "alias"),
		Created:    created.UTC().Format("2006-01-02T15:04:05Z"),
		Charts:     joinOrDash(chartVersions(charts)),
		VerifiedIn: sortedKeys(verified),
	}
}

func chartVersions(list []any) []string {
	var out []string
	for _, c := range list {
		m, _ := c.(map[string]any)
		name, _, _ := unstructured.NestedString(m, "name")
		version, _, _ := unstructured.NestedString(m, "version")
		if name != "" || version != "" {
			out = append(out, name+"@"+version)
		}
	}
	return out
}

func nestedOrDash(obj map[string]any, fields ...string) string {
	v, _, _ := unstructured.NestedString(obj, fields...)
	return orDash(v)
}

func joinOrDash(v []string) string { return orDash(strings.Join(v, ", ")) }

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// RenderRelease formats a release report as Slack mrkdwn.
func RenderRelease(rep ReleaseReport) string {
	var b strings.Builder
	b.WriteString("*Kargo stages*\n```\n")
	fmt.Fprintf(&b, "%-18s %-42s %-28s %-10s %-10s %s\n", "STAGE", "FREIGHT", "CHARTS", "HEALTH", "PROMOTION", "FINISHED")
	for _, s := range rep.Stages {
		fmt.Fprintf(&b, "%-18s %-42s %-28s %-10s %-10s %s\n", s.Name, s.Freight, s.Charts, s.Health, s.PromotionPhase, s.FinishedAt)
	}
	b.WriteString("```\n*Recent freight*\n```\n")
	fmt.Fprintf(&b, "%-42s %-18s %-21s %-28s %s\n", "FREIGHT", "ALIAS", "CREATED", "CHARTS", "VERIFIED IN")
	for _, f := range rep.Freight {
		fmt.Fprintf(&b, "%-42s %-18s %-21s %-28s %s\n", f.Name, f.Alias, f.Created, f.Charts, joinOrDash(f.VerifiedIn))
	}
	b.WriteString("```\n*Running*\n```\n")
	fmt.Fprintf(&b, "%-26s %-18s %s\n", "CLUSTER", "STAGE", "OME")
	for _, c := range rep.Clusters {
		ome := "unreachable"
		if c.Reachable {
			ome = "absent"
			if c.OMEInstalled {
				ome = orDash(c.OMEVersion)
			}
		}
		fmt.Fprintf(&b, "%-26s %-18s %s\n", c.Name, orDash(c.KargoStage), ome)
	}
	b.WriteString("```")
	return b.String()
}
