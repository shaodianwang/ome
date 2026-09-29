//go:build meta_internal

package omebot

import (
	"strings"
	"testing"
)

func TestParseCommand(t *testing.T) {
	cases := []struct {
		text    string
		kind    Kind
		verb    string
		args    string
		cluster string
		wantErr string
	}{
		{text: "", kind: KindHelp},
		{text: "help", kind: KindHelp},
		{text: "installed clusters", kind: KindClusters},
		{text: "clusters", kind: KindClusters},
		{text: "get isvc -A", kind: KindCLI, verb: "get", args: "isvc -A"},
		{text: "get isvc chat -n team-a --cluster member-a", kind: KindCLI, verb: "get", args: "isvc chat -n team-a", cluster: "member-a"},
		{text: "rollout status chat --cluster=member-a -n team-a", kind: KindCLI, verb: "rollout status", args: "chat -n team-a", cluster: "member-a"},
		{text: `logs chat --cluster member-a --container "engine main"`, kind: KindCLI, verb: "logs", args: "chat --container engine main", cluster: "member-a"},
		{text: "rollout status chat", wantErr: "needs `--cluster"},
		{text: "rollout pause chat --cluster member-a", wantErr: "not a read-only"},
		{text: "traffic drain chat --cluster member-a", wantErr: "not a read-only"},
		{text: "scale chat --cluster member-a", wantErr: "not a read-only"},
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
		if p.Kind != tc.kind || strings.Join(p.Verb, " ") != tc.verb || strings.Join(p.Args, " ") != tc.args || p.Cluster != tc.cluster {
			t.Errorf("%q: got %+v", tc.text, p)
		}
	}
}

func TestNoGuardedVerbIsReadable(t *testing.T) {
	for _, guarded := range []string{
		"rollout pause", "rollout resume", "rollout promote", "rollout rollback", "rollout repin",
		"traffic drain", "migration start", "instance release-held", "runtime sync", "scale", "autoscale set",
	} {
		if readVerbs[guarded] {
			t.Errorf("%q must never be in the read allowlist", guarded)
		}
	}
}

func TestHelpTextListsVerbs(t *testing.T) {
	h := HelpText([]string{"member-a"})
	for _, needle := range []string{"installed clusters", "rollout status", "member-a", "guarded verbs are refused"} {
		if !strings.Contains(h, needle) {
			t.Errorf("help lacks %q", needle)
		}
	}
}
