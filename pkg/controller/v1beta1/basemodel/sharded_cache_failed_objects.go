package basemodel

import (
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/modelcache"
)

// mergeFailedObjects keys by Path, preserves FirstSeenAt across rounds,
// always advances LastSeenAt, and accumulates Attempts (prior + fresh).
// Paths absent from `fresh` are dropped — the list reflects currently-
// failing objects, not history (history goes to Events). Capped at
// v1beta1.MaxFailedObjectsReported.
func mergeFailedObjects(existing []v1beta1.FailedObject, fresh []modelcache.ChunkFailure, now time.Time) (merged []v1beta1.FailedObject, total int32, truncated bool) {
	nowTime := metav1.NewTime(now)
	prevByPath := make(map[string]v1beta1.FailedObject, len(existing))
	for _, e := range existing {
		prevByPath[e.Path] = e
	}

	merged = make([]v1beta1.FailedObject, 0, len(fresh))
	for _, f := range fresh {
		entry := v1beta1.FailedObject{
			Path:       f.Path,
			ErrorCode:  f.Code,
			ErrorClass: v1beta1.ErrorClass(f.Class),
			Message:    f.Message,
			LastSeenAt: &nowTime,
		}
		if f.SizeBytes > 0 {
			size := f.SizeBytes
			entry.SizeBytes = &size
		}
		attempts := f.Attempts
		if prev, ok := prevByPath[f.Path]; ok {
			entry.FirstSeenAt = prev.FirstSeenAt
			if prev.Attempts != nil {
				attempts += *prev.Attempts
			}
		} else {
			entry.FirstSeenAt = &nowTime
		}
		entry.Attempts = &attempts
		merged = append(merged, entry)
	}

	sort.SliceStable(merged, func(i, j int) bool {
		ai, bi := time.Time{}, time.Time{}
		if merged[i].FirstSeenAt != nil {
			ai = merged[i].FirstSeenAt.Time
		}
		if merged[j].FirstSeenAt != nil {
			bi = merged[j].FirstSeenAt.Time
		}
		if !ai.Equal(bi) {
			return ai.Before(bi)
		}
		return merged[i].Path < merged[j].Path
	})

	total = int32(len(merged))
	if int(total) > v1beta1.MaxFailedObjectsReported {
		merged = merged[:v1beta1.MaxFailedObjectsReported]
		truncated = true
	}
	return merged, total, truncated
}

func applyFailedObjectsToCache(cache *v1beta1.ModelCacheStatus, merged []v1beta1.FailedObject, total int32, truncated bool) bool {
	if cache == nil {
		return false
	}
	updated := false
	if !equalFailedObjects(cache.FailedObjects, merged) {
		cache.FailedObjects = merged
		updated = true
	}
	if total == 0 {
		if cache.FailedObjectsTotal != nil {
			cache.FailedObjectsTotal = nil
			updated = true
		}
	} else if cache.FailedObjectsTotal == nil || *cache.FailedObjectsTotal != total {
		t := total
		cache.FailedObjectsTotal = &t
		updated = true
	}
	if !truncated {
		if cache.FailedObjectsTruncated != nil {
			cache.FailedObjectsTruncated = nil
			updated = true
		}
	} else if cache.FailedObjectsTruncated == nil || !*cache.FailedObjectsTruncated {
		tr := true
		cache.FailedObjectsTruncated = &tr
		updated = true
	}
	return updated
}

func clearFailedObjectsOnCache(cache *v1beta1.ModelCacheStatus) bool {
	if cache == nil {
		return false
	}
	updated := false
	if cache.FailedObjects != nil {
		cache.FailedObjects = nil
		updated = true
	}
	if cache.FailedObjectsTotal != nil {
		cache.FailedObjectsTotal = nil
		updated = true
	}
	if cache.FailedObjectsTruncated != nil {
		cache.FailedObjectsTruncated = nil
		updated = true
	}
	return updated
}

func equalFailedObjects(a, b []v1beta1.FailedObject) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Path != b[i].Path ||
			a[i].ErrorClass != b[i].ErrorClass ||
			a[i].ErrorCode != b[i].ErrorCode ||
			a[i].Message != b[i].Message {
			return false
		}
		if !int64PtrEq(a[i].SizeBytes, b[i].SizeBytes) ||
			!int32PtrEq(a[i].Attempts, b[i].Attempts) ||
			!timePtrEq(a[i].FirstSeenAt, b[i].FirstSeenAt) ||
			!timePtrEq(a[i].LastSeenAt, b[i].LastSeenAt) {
			return false
		}
	}
	return true
}

func hasSourceObjectMissing(failed []v1beta1.FailedObject) bool {
	for _, f := range failed {
		if f.ErrorClass == v1beta1.ErrorClassSourceObjectMissing {
			return true
		}
	}
	return false
}

// metadataFileBasenames are the files that the parser reads to populate
// BaseModel.spec. When one of these goes missing on the source, the
// MetadataExtracted condition gets the ExtractedFromCache caveat.
var metadataFileBasenames = map[string]struct{}{
	"config.json":                  {},
	"model_index.json":             {},
	"model.safetensors.index.json": {},
	"hf_quant_config.json":         {},
}

func hasMetadataObjectMissing(failed []v1beta1.FailedObject) bool {
	for _, f := range failed {
		if f.ErrorClass != v1beta1.ErrorClassSourceObjectMissing {
			continue
		}
		if _, ok := metadataFileBasenames[f.Path]; ok {
			return true
		}
	}
	return false
}

func int64PtrEq(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func int32PtrEq(a, b *int32) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func timePtrEq(a, b *metav1.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Time.Equal(b.Time)
}
