package omenative_perm_kind

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// TestGeneratedCorpusParses loads EVERY committed perm-harness case YAML
// (hand-written and generated alike) and checks the contract the executors
// depend on: the line-1 header parses with the required replica/ceiling
// fields, the topology tag is a known value, the document set contains an
// InferenceService that decodes into the Go API type under the case's own
// name, and gang topologies declare a worker size (so the CI sharder can
// derive a real pod weight instead of scheduling an exclusive wave).
// Generated cases (gen-* folders) additionally carry a line-2 "# reason:"
// verdict rationale. Pure file parsing — no cluster, no suite bootstrap.
func TestGeneratedCorpusParses(t *testing.T) {
	root := permCaseRoot(t)
	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".yaml") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if len(files) == 0 {
		t.Fatalf("no case YAMLs under %s", root)
	}

	seen := map[string]string{}
	for _, path := range files {
		rel, _ := filepath.Rel(root, path)
		t.Run(rel, func(t *testing.T) {
			id := strings.TrimSuffix(filepath.Base(path), ".yaml")
			if prev, dup := seen[id]; dup {
				t.Fatalf("duplicate case id %s (also %s)", id, prev)
			}
			seen[id] = rel
			checkPermCaseFile(t, path, id, strings.HasPrefix(rel, "gen-"))
		})
	}
}

var permTopologies = map[string]bool{
	"engine": true, "pd": true, "engine-mn": true, "pd-mn": true,
}

var (
	reasonLineRe = regexp.MustCompile(`^\s*#\s*reason:\s*\S`)
	docSplitRe   = regexp.MustCompile(`(?m)^---\s*$`)
	isvcKindRe   = regexp.MustCompile(`(?m)^kind:\s*InferenceService\s*$`)
)

func checkPermCaseFile(t *testing.T, path, id string, generated bool) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(string(raw), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[0], "#") {
		t.Fatalf("line 1 is not the '# <id>: <desc> k=v ...' header comment")
	}
	header := strings.TrimSpace(strings.TrimLeft(lines[0], "#"))
	if !strings.HasPrefix(header, id+":") {
		t.Fatalf("header id does not match filename stem %q: %q", id, lines[0])
	}

	// Forward last-wins k=v scan — the semantics of run.py's parse_header and
	// the pipeline sharder's field extraction.
	fields := map[string]string{}
	for _, tok := range strings.Fields(header) {
		if k, v, ok := strings.Cut(tok, "="); ok && !strings.HasPrefix(tok, "(") {
			fields[k] = v
		}
	}
	for _, k := range []string{"Ne", "Nd", "Nr", "ceil_e", "ceil_d", "ceil_r"} {
		v, ok := fields[k]
		if !ok {
			t.Fatalf("header missing required %s=", k)
		}
		if n, err := strconv.Atoi(v); err != nil || n < 0 {
			t.Fatalf("header %s=%q is not a non-negative integer", k, v)
		}
	}
	topo := fields["topo"]
	if !permTopologies[topo] {
		t.Fatalf("header topo=%q is not a known topology", topo)
	}

	if generated {
		if len(lines) < 2 || !reasonLineRe.MatchString(lines[1]) {
			t.Fatalf("generated case must carry a line-2 '# reason: <why>' rationale")
		}
	}

	isvc := decodePermISVC(t, string(raw))
	if isvc.Name != id {
		t.Fatalf("InferenceService metadata.name %q != case id %q", isvc.Name, id)
	}

	// Gang topologies must declare worker sizes on their gang components, or
	// the sharder cannot weigh the case and parks it on an exclusive wave.
	if strings.HasSuffix(topo, "-mn") {
		if n := genAtoiField(t, fields, "Ne"); n > 0 {
			requireWorker(t, "engine", isvc.Spec.Engine != nil, func() *v1beta1.WorkerSpec {
				return isvc.Spec.Engine.Worker
			})
		}
		if n := genAtoiField(t, fields, "Nd"); n > 0 {
			requireWorker(t, "decoder", isvc.Spec.Decoder != nil, func() *v1beta1.WorkerSpec {
				return isvc.Spec.Decoder.Worker
			})
		}
	}
}

func genAtoiField(t *testing.T, fields map[string]string, k string) int {
	t.Helper()
	n, err := strconv.Atoi(fields[k])
	if err != nil {
		t.Fatalf("header %s=%q: %v", k, fields[k], err)
	}
	return n
}

func requireWorker(t *testing.T, comp string, present bool, worker func() *v1beta1.WorkerSpec) {
	t.Helper()
	if !present {
		t.Fatalf("topo is multi-node but the %s component is absent", comp)
	}
	w := worker()
	if w == nil || w.Size == nil || *w.Size < 1 {
		t.Fatalf("multi-node %s must declare worker.size >= 1 for pod-weight derivation", comp)
	}
}

// decodePermISVC finds the InferenceService document (case files may bundle
// extra documents, e.g. a dedicated ServingRuntime) and decodes it strictly,
// so an unknown or misspelled spec field fails here instead of being silently
// dropped at apply time.
func decodePermISVC(t *testing.T, raw string) *v1beta1.InferenceService {
	t.Helper()
	for _, doc := range docSplitRe.Split(raw, -1) {
		if !isvcKindRe.MatchString(doc) {
			continue
		}
		isvc := &v1beta1.InferenceService{}
		if err := yaml.UnmarshalStrict([]byte(doc), isvc); err != nil {
			t.Fatalf("decode InferenceService: %v", err)
		}
		return isvc
	}
	t.Fatal("no InferenceService document in the case file")
	return nil
}

func permCaseRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(source), "../../../../.."))
	root := filepath.Join(repoRoot, "hack/omenative-testing/perm/isvcs")
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("case tree not found: %v", err)
	}
	return root
}
