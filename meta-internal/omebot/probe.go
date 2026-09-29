//go:build meta_internal

package omebot

import (
	"context"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// omeAPIGroup is the API group whose presence means OME is installed.
const omeAPIGroup = "ome.io"

// ClusterStatus is one row of `installed clusters`.
type ClusterStatus struct {
	Name         string `json:"name"`
	Provider     string `json:"provider"`
	Role         string `json:"role,omitempty"`
	Hub          string `json:"hub,omitempty"`
	KargoStage   string `json:"kargoStage,omitempty"`
	Reachable    bool   `json:"reachable"`
	K8sVersion   string `json:"k8sVersion,omitempty"`
	OMEInstalled bool   `json:"omeInstalled"`
	OMEVersion   string `json:"omeVersion,omitempty"`
	Error        string `json:"error,omitempty"`
	LatencyMS    int64  `json:"latencyMs"`
}

// Prober checks each registry cluster in parallel.
type Prober struct {
	Config  *Config
	Paths   map[string]string
	Timeout time.Duration
}

// restConfig builds the client configuration for one cluster.
func (p *Prober) restConfig(cl Cluster) (*rest.Config, error) {
	var cfg *rest.Config
	var err error
	if cl.InCluster {
		cfg, err = rest.InClusterConfig()
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags("", p.Paths[cl.Name])
	}
	if err != nil {
		return nil, err
	}
	cfg.Timeout = p.Timeout
	return cfg, nil
}

// ProbeAll returns one status per registry cluster, in registry order.
func (p *Prober) ProbeAll(ctx context.Context) []ClusterStatus {
	out := make([]ClusterStatus, len(p.Config.Clusters))
	var wg sync.WaitGroup
	for i, cl := range p.Config.Clusters {
		wg.Add(1)
		go func(i int, cl Cluster) {
			defer wg.Done()
			out[i] = p.probe(ctx, cl)
		}(i, cl)
	}
	wg.Wait()
	return out
}

func (p *Prober) probe(ctx context.Context, cl Cluster) ClusterStatus {
	st := ClusterStatus{Name: cl.Name, Provider: cl.Provider, Role: cl.Role, Hub: cl.Hub, KargoStage: cl.KargoStage}
	start := time.Now()
	defer func() { st.LatencyMS = time.Since(start).Milliseconds() }()

	cfg, err := p.restConfig(cl)
	if err != nil {
		st.Error = Redact(err.Error())
		return st
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		st.Error = Redact(err.Error())
		return st
	}
	ver, err := dc.ServerVersion()
	if err != nil {
		st.Error = shortErr(err)
		return st
	}
	st.Reachable = true
	st.K8sVersion = ver.GitVersion
	groups, err := dc.ServerGroups()
	if err != nil {
		st.Error = shortErr(err)
		return st
	}
	for _, g := range groups.Groups {
		if g.Name == omeAPIGroup {
			st.OMEInstalled = true
			break
		}
	}
	if !st.OMEInstalled || p.Config.OME.Namespace == "" {
		return st
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		st.Error = shortErr(err)
		return st
	}
	dep, err := cs.AppsV1().Deployments(p.Config.OME.Namespace).Get(ctx, p.Config.OME.ManagerDeployment, metav1.GetOptions{})
	if err != nil {
		st.Error = "manager version: " + shortErr(err)
		return st
	}
	if c := dep.Spec.Template.Spec.Containers; len(c) > 0 {
		st.OMEVersion = imageTag(c[0].Image)
	}
	return st
}

// imageTag returns the tag of an image reference, or the digest, or "".
func imageTag(image string) string {
	if i := strings.LastIndex(image, "@"); i >= 0 {
		return image[i+1:]
	}
	slash := strings.LastIndex(image, "/")
	if i := strings.LastIndex(image, ":"); i > slash {
		return image[i+1:]
	}
	return ""
}

// shortErr keeps the first line of a client error; the rest is noise for a
// chat reply and may echo request internals.
func shortErr(err error) string {
	msg := Redact(err.Error())
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	return msg
}
