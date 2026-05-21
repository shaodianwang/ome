package runtimerevision

import "testing"

func TestName_ClusterScope(t *testing.T) {
	got := Name(KindClusterServingRuntime, "", "srt-llama-pd", "abc12345")
	want := "cr-srt-llama-pd-abc12345"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestName_NamespacedScope(t *testing.T) {
	got := Name(KindServingRuntime, "team-a", "srt-foo", "abc12345")
	want := "r-team-a-srt-foo-abc12345"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestName_NoCollisionAcrossNamespaces(t *testing.T) {
	// Same SR name in two namespaces hashes to (different content
	// most likely, but even with identical content) must produce
	// different revision names so they don't clash in the OME ns.
	a := Name(KindServingRuntime, "team-a", "srt-foo", "abc12345")
	b := Name(KindServingRuntime, "team-b", "srt-foo", "abc12345")
	if a == b {
		t.Fatalf("name collision across namespaces: both %q", a)
	}
}

func TestName_UnknownKindIsDistinct(t *testing.T) {
	got := Name(SourceKind("Bogus"), "team-a", "srt-foo", "abc12345")
	if got == Name(KindClusterServingRuntime, "team-a", "srt-foo", "abc12345") {
		t.Fatal("unknown kind silently aliased to cluster name shape")
	}
}
