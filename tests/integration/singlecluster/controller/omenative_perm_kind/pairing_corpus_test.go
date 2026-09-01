package omenative_perm_kind

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// The playground's pairing corpus (hack/omenative-testing/perm/isvcs/pairing)
// is the executable case set for mode=pairing; this loader parses every
// committed case's line-1 header with the same backward-walk semantics as
// run.py and enforces the pairing-mode schema, so a malformed case fails here
// before it ever reaches a cluster lane.

// pairingCase is one parsed corpus case.
type pairingCase struct {
	id     string
	desc   string
	fields map[string]string
	body   string
	reason bool // line 2 carries a '# reason:' comment
}

// pairingHeaderKeys is the closed key set a pairing-corpus header may carry
// (the subset of run.py's HDR_VALIDATORS these cases use).
var pairingHeaderKeys = map[string]bool{
	"mode": true, "Ne": true, "Nd": true, "Nr": true,
	"ceil_e": true, "ceil_d": true, "ceil_r": true, "topo": true, "co": true,
	"proto": true, "proto2": true, "hold": true, "pkill": true,
	"scale_e": true, "scale_d": true, "pause": true, "canary": true,
	"expect": true, "reason": true, "inv_off": true, "order_soft": true,
	"infeasible": true, "notimpl": true, "issue": true,
	"floor_hard": true, "floor_soft": true,
}

// pairingTokenRE mirrors the CRD pattern for spec.rollout.pairingProtocol.
var pairingTokenRE = regexp.MustCompile(`^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`)

func pairingCorpusDir(t *testing.T) string {
	t.Helper()
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this source file")
	}
	// tests/integration/singlecluster/controller/omenative_perm_kind → repo root.
	root := filepath.Join(filepath.Dir(self), "..", "..", "..", "..", "..")
	dir := filepath.Join(root, "hack", "omenative-testing", "perm", "isvcs", "pairing")
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("pairing corpus dir not found: %v", err)
	}
	return dir
}

// parsePairingCase parses one case file's header (tokens walked BACKWARD from
// the end until a token is not key=value with a known, unseen key — the same
// recovery run.py uses so free-text descriptions can't shadow schema fields).
func parsePairingCase(t *testing.T, path string) pairingCase {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	lines := strings.Split(string(raw), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[0], "#") {
		t.Fatalf("%s:1: missing '# <id>: <desc> k=v ...' header comment", path)
	}
	header := strings.TrimSpace(strings.TrimLeft(lines[0], "#"))
	pc := pairingCase{
		id:     strings.TrimSuffix(filepath.Base(path), ".yaml"),
		fields: map[string]string{},
		body:   string(raw),
		reason: len(lines) > 1 && regexp.MustCompile(`^\s*#\s*reason:\s*\S`).MatchString(lines[1]),
	}
	toks := strings.Fields(header)
	cut := len(toks)
	for i := len(toks) - 1; i >= 0; i-- {
		k, v, found := strings.Cut(toks[i], "=")
		if !found || strings.HasPrefix(toks[i], "(") || !pairingHeaderKeys[k] {
			break
		}
		if _, dup := pc.fields[k]; dup {
			break // description territory repeating a real key
		}
		pc.fields[k] = v
		cut = i
	}
	pc.desc = strings.Join(toks[:cut], " ")
	if !strings.HasPrefix(pc.desc, pc.id+":") {
		t.Errorf("%s:1: header id does not match filename stem %q", path, pc.id)
	}
	return pc
}

func atoiField(t *testing.T, pc pairingCase, key string) int {
	t.Helper()
	v, ok := pc.fields[key]
	if !ok {
		t.Errorf("%s: header missing %s=", pc.id, key)
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		t.Errorf("%s: malformed %s=%s", pc.id, key, v)
	}
	return n
}

// TestPairingCorpus loads every committed pairing case and enforces the
// mode=pairing header schema plus the cross-field rules the runner depends on.
// Pure filesystem — no cluster, no KIND.
func TestPairingCorpus(t *testing.T) {
	dir := pairingCorpusDir(t)
	files, err := filepath.Glob(filepath.Join(dir, "p*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no pairing cases under %s (err=%v)", dir, err)
	}
	seen := map[string]bool{}
	for _, path := range files {
		pc := parsePairingCase(t, path)
		if seen[pc.id] {
			t.Errorf("%s: duplicate case id", pc.id)
		}
		seen[pc.id] = true

		if pc.fields["mode"] != "pairing" {
			t.Errorf("%s: pairing corpus case must declare mode=pairing (got %q)", pc.id, pc.fields["mode"])
		}
		nd := atoiField(t, pc, "Nd")
		ne := atoiField(t, pc, "Ne")
		atoiField(t, pc, "Nr")
		for _, k := range []string{"ceil_e", "ceil_d", "ceil_r"} {
			atoiField(t, pc, k)
		}
		switch pc.fields["topo"] {
		case "engine", "pd", "engine-mn", "pd-mn":
		default:
			t.Errorf("%s: bad topo=%q", pc.id, pc.fields["topo"])
		}

		proto, ok := pc.fields["proto"]
		if !ok {
			t.Errorf("%s: mode=pairing needs proto=<token|clear>", pc.id)
		} else if proto != "clear" && !pairingTokenRE.MatchString(proto) {
			t.Errorf("%s: proto=%q is not a CRD-valid pairing token", pc.id, proto)
		}
		if p2, ok := pc.fields["proto2"]; ok {
			if p2 == proto {
				t.Errorf("%s: proto2 must differ from proto", pc.id)
			}
			if p2 != "clear" && !pairingTokenRE.MatchString(p2) {
				t.Errorf("%s: proto2=%q is not a CRD-valid pairing token", pc.id, p2)
			}
		}
		if pc.fields["pkill"] == "1" {
			if nd == 0 {
				t.Errorf("%s: pkill=1 kills the last old-cohort decoder — needs Nd>0", pc.id)
			}
			if pc.fields["inv_off"] != "C" && pc.fields["inv_off"] != "all" {
				t.Errorf("%s: pkill=1 destroys the pair itself — waive pair-alive via inv_off=C", pc.id)
			}
			if pc.fields["order_soft"] != "1" {
				t.Errorf("%s: pkill=1 drains a pod with no preceding surge — needs order_soft=1", pc.id)
			}
		}
		if _, ok := pc.fields["inv_off"]; ok && !pc.reason {
			t.Errorf("%s: inv_off requires an adjacent '# reason:' comment on line 2", pc.id)
		}
		if _, ok := pc.fields["infeasible"]; ok && !pc.reason {
			t.Errorf("%s: infeasible=1 requires an adjacent '# reason:' comment on line 2", pc.id)
		}
		if c, ok := pc.fields["canary"]; ok && c != "auto" {
			t.Errorf("%s: mode=pairing drives canary groups auto-only (canary=%s)", pc.id, c)
		}
		if pc.fields["expect"] == "reject" {
			if _, ok := pc.fields["reason"]; !ok {
				t.Errorf("%s: expect=reject should pin the denial reason code (reason=)", pc.id)
			}
			for _, k := range []string{"proto2", "pkill", "scale_e", "scale_d", "pause", "hold"} {
				if _, ok := pc.fields[k]; ok {
					t.Errorf("%s: expect=reject asserts the patch is denied — %s= does not apply", pc.id, k)
				}
			}
		}

		// The ISVC document run.py fetches must exist under the case id.
		if !strings.Contains(pc.body, fmt.Sprintf("name: %s ", pc.id)) &&
			!strings.Contains(pc.body, fmt.Sprintf("name: %s}", pc.id)) &&
			!strings.Contains(pc.body, fmt.Sprintf("name: %s\n", pc.id)) {
			t.Errorf("%s: no document named %q in the case file", pc.id, pc.id)
		}

		// Webhook-shape lint: a non-empty→non-empty swap with both pairing
		// Components present must roll them as ONE blueGreen or canary group
		// (or be an expect=reject admission negative) — anything else would
		// be denied by ValidatePairingProtocolUpdate at run time.
		from := ""
		if m := regexp.MustCompile(`(?m)^\s+pairingProtocol:\s*(\S+)`).FindStringSubmatch(pc.body); m != nil {
			from = m[1]
		}
		if from != "" && proto != "clear" && proto != from && ne > 0 && nd > 0 && pc.fields["expect"] != "reject" {
			grouped := regexp.MustCompile(`components:\s*\[engine,\s*decoder`).MatchString(pc.body) ||
				regexp.MustCompile(`components:\s*\[decoder,\s*engine`).MatchString(pc.body)
			if !grouped {
				t.Errorf("%s: a %q→%q swap with engine+decoder must declare one group spanning both (or expect=reject)", pc.id, from, proto)
			}
		}
	}
	t.Logf("pairing corpus OK: %d cases", len(files))
}
