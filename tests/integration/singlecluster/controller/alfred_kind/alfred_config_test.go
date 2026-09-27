package alfred_kind

import (
	"reflect"
	"strings"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// configMutator edits alfred's decoded config.yaml document in place.
type configMutator func(cfg map[string]any)

// setConfig sets a dotted path such as "policies.nodeHealth.enabled",
// creating intermediate maps. An intermediate that is not a map (a scalar or
// a list) is replaced by a map, since the path asserts nesting below it.
func setConfig(path string, value any) configMutator {
	return func(cfg map[string]any) {
		parts := strings.Split(path, ".")
		cur := cfg
		for _, p := range parts[:len(parts)-1] {
			next, ok := cur[p].(map[string]any)
			if !ok {
				next = map[string]any{}
				cur[p] = next
			}
			cur = next
		}
		cur[parts[len(parts)-1]] = value
	}
}

// configValue reads the leaf at a dotted path of alfred's decoded config;
// false when a segment is missing or an intermediate is not a map.
func configValue(cfg map[string]any, path string) (any, bool) {
	var cur any = cfg
	for _, p := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[p]; !ok {
			return nil, false
		}
	}
	return cur, true
}

func alfredConfigMap(g gomega.Gomega) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: alfredNamespace, Name: alfredConfigMapName}, cm)).To(gomega.Succeed())
	return cm
}

func loadAlfredConfig(g gomega.Gomega) map[string]any {
	cfg := map[string]any{}
	g.ExpectWithOffset(1, yaml.Unmarshal([]byte(alfredConfigMap(g).Data[alfredConfigKey]), &cfg)).To(gomega.Succeed(), "parse alfred config")
	return cfg
}

func writeAlfredConfig(raw string) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		cm := alfredConfigMap(g)
		cm.Data[alfredConfigKey] = raw
		g.Expect(k8sClient.Update(suiteCtx, cm)).To(gomega.Succeed())
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// awaitReload waits for the leader to count one more successful reload than
// before. Every replica reloads; the leader is the one that decides.
func awaitReload(before float64) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		g.Expect(reloadSuccesses(g)).To(gomega.BeNumerically(">", before), "alfred has not reloaded its config")
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// reloadSuccessesEventually is the leader's successful reload count, read
// with retries.
func reloadSuccessesEventually() float64 { return reloadCountEventually("success") }

// patchAlfredConfig applies mutators to alfred's config, waits for the hot
// reload, and restores the previous document (waiting again) at spec end.
func patchAlfredConfig(mutators ...configMutator) {
	g := gomega.Default
	prev := alfredConfigMap(g).Data[alfredConfigKey]
	cfg := loadAlfredConfig(g)
	for _, m := range mutators {
		m(cfg)
	}
	out, err := yaml.Marshal(cfg)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	before := reloadSuccessesEventually()
	writeAlfredConfig(string(out))
	awaitReload(before)
	ginkgo.DeferCleanup(func() {
		before := reloadSuccessesEventually()
		writeAlfredConfig(prev)
		awaitReload(before)
	})
}

// writeInvalidAlfredConfig writes a document alfred must reject; the previous
// document is restored at spec end (a rejected document never counts as a
// reload, so nothing is awaited on the way in).
func writeInvalidAlfredConfig(raw string) {
	prev := alfredConfigMap(gomega.Default).Data[alfredConfigKey]
	writeAlfredConfig(raw)
	ginkgo.DeferCleanup(func() {
		before := reloadSuccessesEventually()
		writeAlfredConfig(prev)
		awaitReload(before)
	})
}

func setAlfredMode(mode string) { patchAlfredConfig(setConfig("mode", mode)) }

func TestSetConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]any
		path string
		val  any
		want map[string]any
	}{
		{
			name: "creates intermediate maps",
			cfg:  map[string]any{},
			path: "policies.nodeHealth.enabled",
			val:  false,
			want: map[string]any{"policies": map[string]any{"nodeHealth": map[string]any{"enabled": false}}},
		},
		{
			name: "overwrites a leaf and keeps siblings",
			cfg:  map[string]any{"mode": "recommend-only", "logLevel": "info"},
			path: "mode",
			val:  "execute",
			want: map[string]any{"mode": "execute", "logLevel": "info"},
		},
		{
			name: "replaces a non-map intermediate",
			cfg:  map[string]any{"policies": "disabled"},
			path: "policies.defragmentation.enabled",
			val:  true,
			want: map[string]any{"policies": map[string]any{"defragmentation": map[string]any{"enabled": true}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setConfig(tc.path, tc.val)(tc.cfg)
			if !reflect.DeepEqual(tc.cfg, tc.want) {
				t.Fatalf("setConfig(%q) = %#v, want %#v", tc.path, tc.cfg, tc.want)
			}
		})
	}
}

func TestConfigValue(t *testing.T) {
	cfg := map[string]any{
		"mode":     "execute",
		"policies": map[string]any{"defragmentation": map[string]any{"fragmentationThreshold": 0.25}},
	}
	if v, ok := configValue(cfg, "policies.defragmentation.fragmentationThreshold"); !ok || v != 0.25 {
		t.Fatalf("configValue(nested leaf) = %v, %v; want 0.25, true", v, ok)
	}
	if v, ok := configValue(cfg, "mode"); !ok || v != "execute" {
		t.Fatalf("configValue(top-level leaf) = %v, %v; want execute, true", v, ok)
	}
	for _, path := range []string{"policies.nodeHealth.enabled", "mode.execute", "missing"} {
		if v, ok := configValue(cfg, path); ok {
			t.Fatalf("configValue(%q) = %v, true; want absent", path, v)
		}
	}
}
