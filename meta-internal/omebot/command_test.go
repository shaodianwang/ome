//go:build meta_internal

package omebot

import (
	"errors"
	"strings"
	"testing"
)

func TestParseCommand(t *testing.T) {
	cases := []struct {
		text      string
		kind      Kind
		verb      string
		args      string
		cluster   string
		question  string
		mode      string
		guarded   bool
		confirmed bool
		wantErr   string
	}{
		{text: "", kind: KindHelp},
		{text: "help", kind: KindHelp},
		{text: "  Help me  ", kind: KindHelp},
		{text: "help?", kind: KindHelp},
		{text: "?help", kind: KindHelp},
		{text: "--help", kind: KindHelp},
		{text: "ask why is chat slow", kind: KindQuestion, question: "why is chat slow", mode: "ask"},
		{text: "Recipe  scale chat down\nto one replica", kind: KindQuestion, question: "scale chat down\nto one replica", mode: "recipe"},
		{text: "why is chat failing?", kind: KindQuestion, question: "why is chat failing?"},
		{text: "what's wrong with chat?", kind: KindQuestion, question: "what's wrong with chat?"},
		{text: "get isvc -A?", kind: KindQuestion, question: "get isvc -A?"},
		{text: "ask", wantErr: "not a kubectl-ome verb"},
		{text: "installed clusters", kind: KindClusters},
		{text: "clusters", kind: KindClusters},
		{text: "get isvc -A", kind: KindCLI, verb: "get", args: "isvc -A"},
		{text: "get isvc chat -n team-a --cluster member-a", kind: KindCLI, verb: "get", args: "isvc chat -n team-a", cluster: "member-a"},
		{text: "rollout status chat --cluster=member-a -n team-a", kind: KindCLI, verb: "rollout status", args: "chat -n team-a", cluster: "member-a"},
		{text: `logs chat --cluster member-a --container "engine main"`, kind: KindCLI, verb: "logs", args: "chat --container engine main", cluster: "member-a"},
		{text: "rollout status chat", wantErr: "needs `--cluster"},
		{text: "rollout pause chat --cluster member-a", kind: KindCLI, verb: "rollout pause", args: "chat", cluster: "member-a", guarded: true},
		{text: "traffic drain chat --id d1 --workload-cluster c1 --reason test --cluster member-a --yes", kind: KindCLI, verb: "traffic drain",
			args: "chat --id d1 --workload-cluster c1 --reason test --yes", cluster: "member-a", guarded: true, confirmed: true},
		{text: "scale chat --component engine --replicas 2 --yes --cluster=member-a", kind: KindCLI, verb: "scale",
			args: "chat --component engine --replicas 2 --yes", cluster: "member-a", guarded: true, confirmed: true},
		{text: "scale chat --component engine --replicas 2", wantErr: "changes a cluster and needs `--cluster"},
		{text: "get isvc --yes --cluster member-a", wantErr: "--yes is not allowed with a read verb"},
		{text: "autoscale set chat --cluster member-a", wantErr: "not a kubectl-ome verb"},
		{text: "get isvc --kubeconfig=/etc/x --cluster member-a", wantErr: "--kubeconfig"},
		{text: "get isvc --as admin --cluster member-a", wantErr: "--as"},
		{text: "get isvc --cluster", wantErr: "needs a value"},
		{text: "installed pods", wantErr: "unknown command"},
		{text: "get isvc \"unterminated", wantErr: "unbalanced quote"},
	}
	for _, tc := range cases {
		p, err := ParseCommand(tc.text)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("%q: err = %v, want containing %q", tc.text, err, tc.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: unexpected error %v", tc.text, err)
			continue
		}
		if p.Kind != tc.kind || strings.Join(p.Verb, " ") != tc.verb || strings.Join(p.Args, " ") != tc.args || p.Cluster != tc.cluster ||
			p.Question != tc.question || p.QuestionMode != tc.mode || p.Guarded != tc.guarded || p.Confirmed != tc.confirmed {
			t.Errorf("%q: got %+v", tc.text, p)
		}
	}
}

func TestGuardedVerbsAreTheElevenActionsAndNeverReadable(t *testing.T) {
	want := []string{
		"rollout pause", "rollout resume", "rollout promote", "rollout rollback", "rollout repin",
		"traffic drain", "traffic undrain", "migration start", "instance release-held", "runtime sync", "scale",
	}
	if got := GuardedVerbs(); len(got) != len(want) {
		t.Errorf("guarded verbs = %v, want %d", got, len(want))
	}
	for _, guarded := range append(want, "autoscale set") {
		if readVerbs[guarded] {
			t.Errorf("%q must never be in the read allowlist", guarded)
		}
	}
	for _, guarded := range want {
		if !guardedVerbs[guarded] {
			t.Errorf("%q must be a guarded verb", guarded)
		}
	}
}

func TestHelpTextListsVerbs(t *testing.T) {
	h := HelpText([]string{"member-a"}, nil)
	for _, needle := range []string{"installed clusters", "rollout status", "member-a", "no cluster here admits guarded verbs"} {
		if !strings.Contains(h, needle) {
			t.Errorf("help lacks %q", needle)
		}
	}
	h = HelpText([]string{"member-a", "dev-a"}, []string{"dev-a"})
	for _, needle := range []string{"guarded verbs (", "rollout promote", "run only on dev-a", "`--yes`"} {
		if !strings.Contains(h, needle) {
			t.Errorf("help with a writable cluster lacks %q:\n%s", needle, h)
		}
	}
}

func TestParseErrorClasses(t *testing.T) {
	cases := map[string]string{
		"autoscale set chat --cluster member-a":           ErrNotAVerb,
		"installed pods":                                  ErrNotAVerb,
		"get isvc --kubeconfig=/etc/x --cluster member-a": ErrForbiddenFlag,
		"get isvc --as admin":                             ErrForbiddenFlag,
		"get isvc --yes":                                  ErrForbiddenFlag,
		"logs chat":                                       ErrNeedsCluster,
		"rollout pause chat":                              ErrNeedsCluster,
		"get isvc --cluster":                              ErrMalformed,
		"get isvc \"unterminated":                         ErrMalformed,
		"get isvc \x01":                                   ErrMalformed,
		"ask why\x01?":                                    ErrMalformed,
	}
	for text, want := range cases {
		_, err := ParseCommand(text)
		var pe *ParseError
		if !errors.As(err, &pe) || pe.Class != want {
			t.Errorf("%q: err = %v, want class %q", text, err, want)
		}
	}
}

func TestReadOnlyPlacementTrafficQuotaVerbs(t *testing.T) {
	for _, text := range []string{
		"placement status chat --cluster member-a",
		"placement explain chat --cluster member-a",
		"traffic status chat --cluster member-a",
		"quota status --cluster member-a",
	} {
		if p, err := ParseCommand(text); err != nil || p.Kind != KindCLI {
			t.Errorf("%q: got %+v, %v", text, p, err)
		}
	}
	if n := len(ReadVerbs()); n != 31 {
		t.Errorf("read verbs = %d, want 31", n)
	}
}
