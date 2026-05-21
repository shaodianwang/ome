package runtimerevision

import "fmt"

// SourceKind identifies which runtime CRD a revision was captured
// from. Used as a label value and as the name-prefix discriminator.
type SourceKind string

const (
	KindClusterServingRuntime SourceKind = "ClusterServingRuntime"
	KindServingRuntime        SourceKind = "ServingRuntime"
)

// Name builds the revision's metadata.name from the source runtime's
// scope, namespace, name, plus the 8-char content hash. All revisions
// live in the OME system namespace; the prefix and embedded
// source-namespace prevent collisions between
//
//	team-a/srt-foo  and  team-b/srt-foo
func Name(kind SourceKind, sourceNamespace, runtimeName, shortHash string) string {
	switch kind {
	case KindClusterServingRuntime:
		return fmt.Sprintf("cr-%s-%s", runtimeName, shortHash)
	case KindServingRuntime:
		return fmt.Sprintf("r-%s-%s-%s", sourceNamespace, runtimeName, shortHash)
	default:
		// Defensive: a typo'd SourceKind shouldn't silently produce
		// the cluster-scoped shape; surface it.
		return fmt.Sprintf("unknown-%s-%s-%s", sourceNamespace, runtimeName, shortHash)
	}
}
