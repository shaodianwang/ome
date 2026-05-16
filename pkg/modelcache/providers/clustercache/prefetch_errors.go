package clustercache

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/ome-projects/ome/pkg/modelcache"
)

// parseChunkErrorStrings extracts structured per-object failures from the
// daemon's free-form error list (ownedChunkPrefetchResponse.Errors). This
// regex-based reconstruction exists because the cluster_cache protocol
// returns []string for per-chunk errors; once the daemon exposes
// structured errors this file goes away.
//
// Recognized formats:
//
//   - Daemon S3 fetch errors:
//     "GET range=bytes=N-M expected_mtime=T s3://bucket/path/<obj>:
//     service error; status=NNN; code=Word; message=..."
//   - Controller-side lookup errors (groupChunksByOwner):
//     "<path>: not listed by cluster_cache file_metadata"
//     "<path>: head fallback for mtime: <err>"
//     "<path>@<offset>: ring lookup: <err>"
//
// Per-owner network errors don't carry a source-object path and are
// handled separately by the caller (aggregateOwnerOutcomes), not here.
func parseChunkErrorStrings(errs []string) []modelcache.ChunkFailure {
	if len(errs) == 0 {
		return nil
	}
	byPath := map[string]*modelcache.ChunkFailure{}
	for _, raw := range errs {
		failure := parseChunkErrorString(raw)
		if existing, ok := byPath[failure.Path]; ok {
			existing.Attempts++
			// Prefer the classified entry and the longer message (the
			// more detailed daemon error tends to win in practice).
			if existing.Class == modelcache.ChunkFailureClassUnknown && failure.Class != modelcache.ChunkFailureClassUnknown {
				existing.Class = failure.Class
				existing.Code = failure.Code
			}
			if len(failure.Message) > len(existing.Message) {
				existing.Message = failure.Message
			}
			continue
		}
		failure.Attempts = 1
		byPath[failure.Path] = &failure
	}
	out := make([]modelcache.ChunkFailure, 0, len(byPath))
	for _, f := range byPath {
		out = append(out, *f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

var (
	chunkErrorURLRE     = regexp.MustCompile(`\b([a-z][a-z0-9+.-]*://[^\s:]+)`)
	chunkErrorStatusRE  = regexp.MustCompile(`\bstatus=(\d{3})\b`)
	chunkErrorCodeRE    = regexp.MustCompile(`\bcode=([A-Za-z][A-Za-z0-9_]*)`)
	chunkErrorMessageRE = regexp.MustCompile(`(?s)\bmessage=(.*?)(?:\s+See\s+https://| body=<|$)`)
	lookupErrorPathRE   = regexp.MustCompile(`^([^\s:@]+)(?:@\d+)?:\s+(.+)$`)
)

// maxChunkErrorMessageBytes caps FailedObject.Message to stay under the
// CRD's MaxLength=512 with headroom for the UTF-8 ellipsis.
const maxChunkErrorMessageBytes = 480

func parseChunkErrorString(raw string) modelcache.ChunkFailure {
	raw = strings.TrimSpace(raw)
	failure := modelcache.ChunkFailure{
		Class:   modelcache.ChunkFailureClassUnknown,
		Message: raw,
	}

	if loc := chunkErrorURLRE.FindStringSubmatchIndex(raw); loc != nil {
		failure.Path = pathFromURL(raw[loc[2]:loc[3]])
	}
	if m := chunkErrorStatusRE.FindStringSubmatch(raw); m != nil {
		if status, err := strconv.Atoi(m[1]); err == nil {
			failure.Class = classFromHTTPStatus(status)
		}
	}
	if m := chunkErrorCodeRE.FindStringSubmatch(raw); m != nil {
		failure.Code = m[1]
		if cls := classFromProviderCode(m[1]); cls != modelcache.ChunkFailureClassUnknown {
			failure.Class = cls
		}
	}
	if m := chunkErrorMessageRE.FindStringSubmatch(raw); m != nil {
		failure.Message = oneLine(m[1])
	} else if failure.Code != "" {
		failure.Message = synthesizeMessage(failure.Code, raw)
	}

	// Lookup-form errors don't have a URL — fall back to the path prefix.
	if failure.Path == "" {
		if m := lookupErrorPathRE.FindStringSubmatch(raw); m != nil {
			failure.Path = m[1]
			failure.Message = oneLine(m[2])
		}
	}

	// groupChunksByOwner sentinel: classify even if the URL regex won.
	if strings.Contains(raw, "not listed by cluster_cache") {
		failure.Class = modelcache.ChunkFailureClassSourceObjectMissing
		failure.Code = "NotInListing"
	}

	if len(failure.Message) > maxChunkErrorMessageBytes {
		failure.Message = failure.Message[:maxChunkErrorMessageBytes] + "…"
	}
	return failure
}

// pathFromURL returns the bucket-relative object path from a URL like
// "s3://bucket/dir/config.json" → "dir/config.json". Returns the full
// path — NOT just the basename — so two distinct objects at
// "dir-a/config.json" and "dir-b/config.json" don't collapse into one
// FailedObject in parseChunkErrorStrings.
func pathFromURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Path == "" {
		return ""
	}
	return strings.TrimLeft(parsed.Path, "/")
}

// bucketRelativePath normalizes a /file_metadata "path" field (always an
// absolute URL like "s3://bucket/dir/file.json") to the bucket-relative
// form pathFromURL returns. Callers stitch fields keyed by this form.
func bucketRelativePath(p string) string {
	if !strings.Contains(p, "://") {
		return strings.TrimLeft(p, "/")
	}
	return pathFromURL(p)
}

func classFromHTTPStatus(status int) modelcache.ChunkFailureClass {
	switch status {
	case 401, 403:
		return modelcache.ChunkFailureClassAuthDenied
	case 404, 410:
		return modelcache.ChunkFailureClassSourceObjectMissing
	case 429:
		return modelcache.ChunkFailureClassThrottled
	}
	if status >= 500 {
		return modelcache.ChunkFailureClassBackendUnreachable
	}
	return modelcache.ChunkFailureClassUnknown
}

func classFromProviderCode(code string) modelcache.ChunkFailureClass {
	switch code {
	case "NoSuchKey", "NoSuchBucket", "KeyNotFound":
		return modelcache.ChunkFailureClassSourceObjectMissing
	case "AccessDenied", "Unauthorized", "Forbidden":
		return modelcache.ChunkFailureClassAuthDenied
	case "SlowDown", "TooManyRequests":
		return modelcache.ChunkFailureClassThrottled
	case "ConnectionRefused", "DNSError", "TLSHandshakeError":
		return modelcache.ChunkFailureClassBackendUnreachable
	}
	return modelcache.ChunkFailureClassUnknown
}

func oneLine(s string) string {
	s = strings.TrimSpace(s)
	if idx := strings.Index(s, "<?xml"); idx >= 0 {
		s = strings.TrimSpace(s[:idx])
	}
	return strings.Join(strings.Fields(s), " ")
}

func synthesizeMessage(code, raw string) string {
	if m := chunkErrorStatusRE.FindStringSubmatch(raw); m != nil {
		return fmt.Sprintf("status=%s code=%s", m[1], code)
	}
	return fmt.Sprintf("code=%s", code)
}
