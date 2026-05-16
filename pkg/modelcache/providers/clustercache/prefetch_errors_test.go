package clustercache

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ome-projects/ome/pkg/modelcache"
)

// The daemon error string seen in the wild that motivated this whole
// status redesign. Verbatim from the msl-cw-use2-2 cluster-cache pod
// logs at 2026-05-16T13:03:50Z. If the parser ever stops handling this
// exact shape, the operator-debug experience regresses.
const realWorldManifold404 = `GET range=bytes=0-5652 expected_mtime=1778725277 ` +
	`s3://fair_llms_prod/tree/vllm_test/avo_integ_tier5_42b_b1_128k_rc3p2_agent_reasoning_joint-dznkgwsc/checkpoint_0000260/checkpoint_ptq/config.json: ` +
	`service error; status=404; code=NoSuchKey; ` +
	`message=Failed to read from Manifold: [404] Path not found -- ` +
	`tree/vllm_test/avo_integ_tier5_42b_b1_128k_rc3p2_agent_reasoning_joint-dznkgwsc/checkpoint_0000260/checkpoint_ptq/config.json ` +
	`See https://fburl.com/mf_404_not_found to troubleshoot. ` +
	`Context: ManifoldSRClient::getReadPlan; request_id=f1dd2a77-1d56-48fe-a246-9d5867067349; ` +
	`extended_request_id=dHNwX2xkYy9hbnlzdG9yZS9wcm94eS5wcm9kLnNoYXJlZC8xNTQy; ` +
	`body=<?xml version="1.0"?> <Error> <Code>NoSuchKey</Code> <Message>Failed to read from Manifold: [404] Path not found -- ` +
	`tree/vllm_test/avo_integ_tier5_42b_b1_128k_rc3p2_agent_reasoning_joint-dznkgwsc/checkpoint_0000260/checkpoint_ptq/config.json ` +
	`See https://fburl.com/mf_404_not_found to troubleshoot. Context: ManifoldSRClient::getReadPlan</Message> </Error>`

func TestParseChunkErrorString_RealWorld404(t *testing.T) {
	f := parseChunkErrorString(realWorldManifold404)

	// Full S3 key (bucket is the URL Host, key is the Path) — NOT just
	// the basename — so two distinct objects at different prefixes within
	// the same bucket don't collide.
	assert.Equal(t, "tree/vllm_test/avo_integ_tier5_42b_b1_128k_rc3p2_agent_reasoning_joint-dznkgwsc/checkpoint_0000260/checkpoint_ptq/config.json", f.Path)
	assert.Equal(t, modelcache.ChunkFailureClassSourceObjectMissing, f.Class)
	assert.Equal(t, "NoSuchKey", f.Code)
	// Message must NOT contain the XML body, request_id, or fburl link.
	assert.NotContains(t, f.Message, "<?xml")
	assert.NotContains(t, f.Message, "request_id")
	assert.NotContains(t, f.Message, "extended_request_id")
	assert.NotContains(t, f.Message, "ManifoldSRClient")
	// Should contain the human-readable message head.
	assert.Contains(t, f.Message, "[404] Path not found")
	// And must fit in the CRD's MaxLength budget.
	assert.LessOrEqual(t, len(f.Message), 480)
}

func TestParseChunkErrorString_ClassificationByStatus(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		expClass modelcache.ChunkFailureClass
		expCode  string
	}{
		{
			name:     "404 → SourceObjectMissing",
			raw:      `s3://b/x: service error; status=404; code=NoSuchKey; message=gone`,
			expClass: modelcache.ChunkFailureClassSourceObjectMissing,
			expCode:  "NoSuchKey",
		},
		{
			name:     "403 → AuthDenied",
			raw:      `s3://b/x: service error; status=403; code=AccessDenied; message=denied`,
			expClass: modelcache.ChunkFailureClassAuthDenied,
			expCode:  "AccessDenied",
		},
		{
			name:     "401 → AuthDenied",
			raw:      `s3://b/x: service error; status=401; code=Unauthorized; message=auth`,
			expClass: modelcache.ChunkFailureClassAuthDenied,
			expCode:  "Unauthorized",
		},
		{
			name:     "429 → Throttled",
			raw:      `s3://b/x: service error; status=429; code=SlowDown; message=slow down`,
			expClass: modelcache.ChunkFailureClassThrottled,
			expCode:  "SlowDown",
		},
		{
			name:     "500 → BackendUnreachable",
			raw:      `s3://b/x: service error; status=500; code=InternalError; message=oops`,
			expClass: modelcache.ChunkFailureClassBackendUnreachable,
			expCode:  "InternalError",
		},
		{
			name:     "status alone with no code still classifies",
			raw:      `s3://b/x: service error; status=403; message=denied`,
			expClass: modelcache.ChunkFailureClassAuthDenied,
		},
		{
			name:     "no status, just code still classifies",
			raw:      `s3://b/x: code=NoSuchKey; message=gone`,
			expClass: modelcache.ChunkFailureClassSourceObjectMissing,
			expCode:  "NoSuchKey",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := parseChunkErrorString(tc.raw)
			assert.Equal(t, tc.expClass, f.Class)
			if tc.expCode != "" {
				assert.Equal(t, tc.expCode, f.Code)
			}
		})
	}
}

func TestParseChunkErrorString_LookupErrors(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		expPath  string
		expClass modelcache.ChunkFailureClass
		expCode  string
	}{
		{
			name:     "missing-from-listing",
			raw:      "config.json: not listed by cluster_cache file_metadata",
			expPath:  "config.json",
			expClass: modelcache.ChunkFailureClassSourceObjectMissing,
			expCode:  "NotInListing",
		},
		{
			name:     "head-fallback failure",
			raw:      "weights.safetensors: head fallback for mtime: connection refused",
			expPath:  "weights.safetensors",
			expClass: modelcache.ChunkFailureClassUnknown,
		},
		{
			name:     "ring-lookup failure",
			raw:      "weights.safetensors@8388608: ring lookup: no owner for key",
			expPath:  "weights.safetensors",
			expClass: modelcache.ChunkFailureClassUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := parseChunkErrorString(tc.raw)
			assert.Equal(t, tc.expPath, f.Path)
			assert.Equal(t, tc.expClass, f.Class)
			if tc.expCode != "" {
				assert.Equal(t, tc.expCode, f.Code)
			}
		})
	}
}

func TestParseChunkErrorString_UnparseableFallback(t *testing.T) {
	// A completely opaque error string should still produce a usable
	// ChunkFailure (Path empty, Class Unknown, Message=raw).
	raw := "something went wrong with no recognizable structure"
	f := parseChunkErrorString(raw)
	assert.Equal(t, "", f.Path)
	assert.Equal(t, modelcache.ChunkFailureClassUnknown, f.Class)
	assert.Equal(t, raw, f.Message)
}

func TestParseChunkErrorString_MessageBoundedAt480(t *testing.T) {
	// Synthesize an error with a 2KB "message=" payload; must be truncated.
	// The ellipsis "…" is a 3-byte UTF-8 codepoint, so the upper bound is
	// 480 + 3 = 483 bytes — comfortably under the CRD's MaxLength=512.
	huge := strings.Repeat("x", 2048)
	raw := "s3://b/x: status=500; code=ServerError; message=" + huge
	f := parseChunkErrorString(raw)
	assert.LessOrEqual(t, len(f.Message), 483, "must be capped (480 chars + UTF-8 ellipsis)")
	assert.True(t, strings.HasSuffix(f.Message, "…"), "must end with ellipsis when truncated")
}

func TestParseChunkErrorStrings_CoalescesByPath(t *testing.T) {
	// Two chunks of the same file fail → one entry with Attempts=2.
	errs := []string{
		`s3://b/weights.safetensors: status=404; code=NoSuchKey; message=gone (chunk 0)`,
		`s3://b/weights.safetensors: status=404; code=NoSuchKey; message=gone (chunk 1)`,
	}
	failures := parseChunkErrorStrings(errs)
	require.Len(t, failures, 1)
	assert.Equal(t, "weights.safetensors", failures[0].Path)
	assert.Equal(t, int32(2), failures[0].Attempts)
	assert.Equal(t, modelcache.ChunkFailureClassSourceObjectMissing, failures[0].Class)
}

func TestParseChunkErrorStrings_DistinctPathsKept(t *testing.T) {
	errs := []string{
		`s3://b/config.json: status=404; code=NoSuchKey; message=gone`,
		`s3://b/weights.safetensors: status=500; code=ServerError; message=oops`,
		`tokenizer.json: not listed by cluster_cache file_metadata`,
	}
	failures := parseChunkErrorStrings(errs)
	require.Len(t, failures, 3)
	// Sorted by Path for stable output.
	assert.Equal(t, "config.json", failures[0].Path)
	assert.Equal(t, "tokenizer.json", failures[1].Path)
	assert.Equal(t, "weights.safetensors", failures[2].Path)
}

func TestParseChunkErrorStrings_DistinctPrefixesDoNotCollide(t *testing.T) {
	// Two objects share a basename but live in different prefixes. They
	// MUST surface as two FailedObject entries; if pathFromURL returned
	// path.Base, they'd silently merge.
	errs := []string{
		`s3://b/dir-a/config.json: status=404; code=NoSuchKey; message=gone`,
		`s3://b/dir-b/config.json: status=404; code=NoSuchKey; message=gone`,
	}
	failures := parseChunkErrorStrings(errs)
	require.Len(t, failures, 2, "different prefixes must NOT collapse into one entry")
	assert.Equal(t, "dir-a/config.json", failures[0].Path)
	assert.Equal(t, "dir-b/config.json", failures[1].Path)
}

func TestParseChunkErrorStrings_PrefersClassifiedOverUnknown(t *testing.T) {
	// Same path: one error is unclassified, second is structured.
	// Coalesced entry should pick up the classification + code.
	errs := []string{
		`s3://b/config.json: something weird happened`,
		`s3://b/config.json: status=404; code=NoSuchKey; message=gone`,
	}
	failures := parseChunkErrorStrings(errs)
	require.Len(t, failures, 1)
	assert.Equal(t, modelcache.ChunkFailureClassSourceObjectMissing, failures[0].Class)
	assert.Equal(t, "NoSuchKey", failures[0].Code)
	assert.Equal(t, int32(2), failures[0].Attempts)
}

func TestSummarizePrewarmFailure_SingleObjectIncludesCode(t *testing.T) {
	failed := []modelcache.ChunkFailure{
		{Path: "config.json", Code: "NoSuchKey", Class: modelcache.ChunkFailureClassSourceObjectMissing},
	}
	msg := summarizePrewarmFailure(failed, nil, 5722, 5721)
	assert.Equal(t, "1/5722 chunks unfetchable: config.json (NoSuchKey)", msg)
}

func TestSummarizePrewarmFailure_MultipleObjects(t *testing.T) {
	failed := []modelcache.ChunkFailure{
		{Path: "a.json", Code: "NoSuchKey", Class: modelcache.ChunkFailureClassSourceObjectMissing},
		{Path: "b.json", Code: "NoSuchKey", Class: modelcache.ChunkFailureClassSourceObjectMissing},
		{Path: "c.json", Code: "AccessDenied", Class: modelcache.ChunkFailureClassAuthDenied},
	}
	msg := summarizePrewarmFailure(failed, nil, 100, 95)
	// Dominant code (NoSuchKey, 2 of 3) should be the hint.
	assert.Contains(t, msg, "5/100 chunks unfetchable across 3 objects")
	assert.Contains(t, msg, "NoSuchKey")
}

func TestSummarizePrewarmFailure_OwnerOnlyFailure(t *testing.T) {
	msg := summarizePrewarmFailure(nil, []string{"10.0.0.1: connection refused"}, 0, 0)
	assert.Equal(t, "10.0.0.1: connection refused", msg)
}

func TestSummarizePrewarmFailure_MixedReportsBothCounts(t *testing.T) {
	failed := []modelcache.ChunkFailure{
		{Path: "config.json", Code: "NoSuchKey"},
	}
	msg := summarizePrewarmFailure(failed, []string{"10.0.0.1: connection refused"}, 100, 99)
	assert.Contains(t, msg, "1/100 chunks unfetchable across 1 objects")
	assert.Contains(t, msg, "1 owner(s) reported errors")
}
