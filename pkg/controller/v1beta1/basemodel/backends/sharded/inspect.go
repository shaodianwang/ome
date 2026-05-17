package sharded

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sync/errgroup"
)

const (
	defaultConfigFileName            = "config.json"
	defaultModelIndexFileName        = "model_index.json"
	defaultSafetensorsIndexFileName  = "model.safetensors.index.json"
	defaultHFQuantConfigFileName     = "hf_quant_config.json"
	maxClusterCacheMetadataFileBytes = 64 * 1024 * 1024
	maxClusterCacheSafetensorHeader  = 10 * 1024 * 1024
)

type packedByteRangeSpec struct {
	Path        string `json:"path"`
	ChunkOffset uint64 `json:"chunk_offset"`
	MTime       uint64 `json:"mtime"`
	ChunkLength uint64 `json:"chunk_length"`
	RangeOffset uint64 `json:"range_offset"`
	Length      uint64 `json:"length"`
}

type packedByteRangeBatchRequest struct {
	Ranges []packedByteRangeSpec `json:"ranges"`
}

type clusterCacheMetadataFile struct {
	path         string
	relativePath string
	size         uint64
	mtime        *uint64
	version      string
}

func (c *clusterCacheClient) inspectSourceFromFileMetadata(ctx context.Context, source Source, metadata *fileMetadataResponse) (*SourceInspection, error) {
	files := metadataFilesFromResponse(source, metadata)
	if len(files) == 0 {
		return nil, fmt.Errorf("cluster_cache file_metadata returned no files for model source: %w", ErrSourceUnparseable)
	}

	// Parser requires config.json or model_index.json — surface the
	// missing-precondition error before any HTTP work.
	if selectMetadataFileByBase(files, defaultConfigFileName) == nil &&
		selectMetadataFileByBase(files, defaultModelIndexFileName) == nil {
		return nil, fmt.Errorf("cluster_cache source inspection requires %s or %s: %w", defaultConfigFileName, defaultModelIndexFileName, ErrSourceUnparseable)
	}

	inputs, err := c.readMetadataInputs(ctx, files)
	if err != nil {
		return nil, err
	}

	inspection := metadata.toModelCacheSourceInspection(source)
	inspection.Metadata = inputs
	return inspection, nil
}

func metadataFilesFromResponse(source Source, response *fileMetadataResponse) []clusterCacheMetadataFile {
	if response == nil {
		return nil
	}

	root := firstNonEmpty(response.Root, source.URI)
	files := make([]clusterCacheMetadataFile, 0, len(response.Files))
	for _, entry := range response.Files {
		objectPath := inspectPath(root, entry.Path, entry.RelativePath)
		if objectPath == "" {
			continue
		}
		// cluster_cache emits relative_path as the bucket key (e.g.
		// "models/llama-7b/config.json"), not model-root-relative. The
		// safetensors index references shards by *root-relative* path
		// ("model-00001-of-00001.safetensors"), so we must derive the
		// relative path from response.Root + entry.Path. Fall back to
		// entry.RelativePath only if the root-strip yields nothing — a
		// defensive last resort that the production cluster_cache shape
		// shouldn't reach.
		relativePath := relativePathFromRoot(root, objectPath)
		if relativePath == "" {
			relativePath = cleanMetadataRelativePath(entry.RelativePath)
		}
		if relativePath == "" {
			continue
		}
		files = append(files, clusterCacheMetadataFile{
			path:         objectPath,
			relativePath: relativePath,
			size:         entry.Size,
			mtime:        entry.MTime,
			version:      inspectVersion("", stringPtrValue(entry.ETag), entry.MTime),
		})
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].relativePath < files[j].relativePath
	})
	return files
}

func relativePathFromRoot(root, objectPath string) string {
	root = strings.TrimRight(strings.TrimSpace(filepath.ToSlash(root)), "/")
	objectPath = strings.TrimSpace(filepath.ToSlash(objectPath))
	if root != "" && strings.HasPrefix(objectPath, root+"/") {
		return cleanMetadataRelativePath(strings.TrimPrefix(objectPath, root+"/"))
	}
	if parsed, err := url.Parse(objectPath); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		return cleanMetadataRelativePath(strings.TrimLeft(parsed.Path, "/"))
	}
	return cleanMetadataRelativePath(objectPath)
}

func cleanMetadataRelativePath(value string) string {
	value = strings.TrimLeft(strings.TrimSpace(filepath.ToSlash(value)), "/")
	if value == "" {
		return ""
	}
	value = path.Clean(value)
	if value == "." || value == ".." || strings.HasPrefix(value, "../") {
		return ""
	}
	return value
}

func selectMetadataFileByBase(files []clusterCacheMetadataFile, baseName string) *clusterCacheMetadataFile {
	var selected *clusterCacheMetadataFile
	for i := range files {
		if path.Base(files[i].relativePath) != baseName {
			continue
		}
		if files[i].relativePath == baseName {
			return &files[i]
		}
		if selected == nil || len(files[i].relativePath) < len(selected.relativePath) {
			selected = &files[i]
		}
	}
	return selected
}

func selectMetadataFileByRelativePath(files []clusterCacheMetadataFile, relativePath string) *clusterCacheMetadataFile {
	relativePath = cleanMetadataRelativePath(relativePath)
	for i := range files {
		if files[i].relativePath == relativePath {
			return &files[i]
		}
	}
	return nil
}

func (c *clusterCacheClient) readMetadataInputs(ctx context.Context, files []clusterCacheMetadataFile) (ModelMetadataInputs, error) {
	inputs := ModelMetadataInputs{}

	if file := selectMetadataFileByBase(files, defaultModelIndexFileName); file != nil {
		data, err := c.readWholeMetadataObject(ctx, file)
		if err != nil {
			return inputs, fmt.Errorf("read %s through cluster_cache: %w", defaultModelIndexFileName, err)
		}
		inputs.ModelIndexJSON = data
		inputs.ModelIndexPath = file.relativePath
	}
	if file := selectMetadataFileByBase(files, defaultConfigFileName); file != nil {
		data, err := c.readWholeMetadataObject(ctx, file)
		if err != nil {
			return inputs, fmt.Errorf("read %s through cluster_cache: %w", defaultConfigFileName, err)
		}
		inputs.ConfigJSON = data
		inputs.ConfigPath = file.relativePath
	}
	if file := selectMetadataFileByBase(files, defaultSafetensorsIndexFileName); file != nil {
		data, err := c.readWholeMetadataObject(ctx, file)
		if err != nil {
			return inputs, fmt.Errorf("read %s through cluster_cache: %w", defaultSafetensorsIndexFileName, err)
		}
		inputs.SafetensorsIndexJSON = data
		inputs.SafetensorsIndexPath = file.relativePath
	}
	// hf_quant_config.json is the NVIDIA ModelOpt / TensorRT-LLM
	// fallback for quantization metadata that doesn't live in
	// config.json. Optional; absence is normal and not an error.
	if file := selectMetadataFileByBase(files, defaultHFQuantConfigFileName); file != nil {
		data, err := c.readWholeMetadataObject(ctx, file)
		if err != nil {
			return inputs, fmt.Errorf("read %s through cluster_cache: %w", defaultHFQuantConfigFileName, err)
		}
		inputs.HFQuantConfigJSON = data
		inputs.HFQuantConfigPath = file.relativePath
	}

	safetensorFiles, err := safetensorMetadataFiles(files, selectMetadataFileByBase(files, defaultSafetensorsIndexFileName), inputs.SafetensorsIndexJSON)
	if err != nil {
		return inputs, err
	}
	headers, err := c.readSafetensorHeaders(ctx, safetensorFiles)
	if err != nil {
		return inputs, err
	}
	inputs.SafetensorHeaders = headers
	return inputs, nil
}

// readSafetensorHeaders fans the per-shard header reads out across a
// bounded errgroup. For a 128-shard model the sequential path was
// ~256 ordered HTTP round-trips; with the default concurrency of 16
// the wall time drops roughly proportionally. Output order matches
// the input order so the parser sees a stable shard listing.
func (c *clusterCacheClient) readSafetensorHeaders(ctx context.Context, files []clusterCacheMetadataFile) ([]SafetensorHeader, error) {
	if len(files) == 0 {
		return nil, nil
	}

	results := make([]SafetensorHeader, len(files))
	group, gctx := errgroup.WithContext(ctx)
	group.SetLimit(c.headerConcurrency)
	for i := range files {
		i := i
		group.Go(func() error {
			header, err := c.readSafetensorHeader(gctx, &files[i])
			if err != nil {
				return err
			}
			results[i] = *header
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func safetensorMetadataFiles(files []clusterCacheMetadataFile, indexFile *clusterCacheMetadataFile, indexJSON []byte) ([]clusterCacheMetadataFile, error) {
	if len(indexJSON) > 0 {
		indexPath := defaultSafetensorsIndexFileName
		if indexFile != nil {
			indexPath = indexFile.relativePath
		}
		refs, err := safetensorsIndexShardPaths(indexJSON, indexPath)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", defaultSafetensorsIndexFileName, err)
		}
		selected := make([]clusterCacheMetadataFile, 0, len(refs))
		for _, ref := range refs {
			file := selectMetadataFileByRelativePath(files, ref)
			if file == nil {
				return nil, fmt.Errorf("safetensors shard %q referenced by %s was not listed by cluster_cache: %w", ref, defaultSafetensorsIndexFileName, ErrSourceUnparseable)
			}
			selected = append(selected, *file)
		}
		return selected, nil
	}

	selected := make([]clusterCacheMetadataFile, 0)
	for _, file := range files {
		if strings.HasSuffix(path.Base(file.relativePath), ".safetensors") {
			selected = append(selected, file)
		}
	}
	return selected, nil
}

func safetensorsIndexShardPaths(data []byte, indexPath string) ([]string, error) {
	var index struct {
		WeightMap map[string]string `json:"weight_map"`
	}
	if err := json.Unmarshal(data, &index); err != nil {
		return nil, err
	}
	if len(index.WeightMap) == 0 {
		return nil, fmt.Errorf("weight_map is empty: %w", ErrSourceUnparseable)
	}

	seen := map[string]struct{}{}
	shards := make([]string, 0, len(index.WeightMap))
	indexDir := path.Dir(cleanMetadataRelativePath(indexPath))
	for _, shard := range index.WeightMap {
		shard = cleanMetadataRelativePath(shard)
		if shard == "" {
			return nil, fmt.Errorf("weight_map includes an empty or invalid shard path: %w", ErrSourceUnparseable)
		}
		if indexDir != "." && indexDir != "" {
			shard = path.Join(indexDir, shard)
		}
		if _, ok := seen[shard]; ok {
			continue
		}
		seen[shard] = struct{}{}
		shards = append(shards, shard)
	}
	sort.Strings(shards)
	return shards, nil
}

// readWholeMetadataObject fetches a metadata file in one shot via
// /remote_object. Guarded by maxClusterCacheMetadataFileBytes.
func (c *clusterCacheClient) readWholeMetadataObject(ctx context.Context, file *clusterCacheMetadataFile) ([]byte, error) {
	if file.size > maxClusterCacheMetadataFileBytes {
		return nil, fmt.Errorf("metadata file %s is %d bytes, above limit %d: %w", file.path, file.size, maxClusterCacheMetadataFileBytes, ErrSourceUnparseable)
	}
	return c.remoteObject(ctx, file.path)
}

// readSafetensorHeader pulls the safetensors header (8-byte length
// prefix + JSON header) via two /packed_byte_ranges_by_key calls to
// the gateway. The daemon picks the source itself.
func (c *clusterCacheClient) readSafetensorHeader(ctx context.Context, file *clusterCacheMetadataFile) (*SafetensorHeader, error) {
	if file.size < 8 {
		return nil, fmt.Errorf("safetensors file %s is too small to contain a header length: %w", file.path, ErrSourceUnparseable)
	}
	if file.mtime == nil {
		return nil, fmt.Errorf("safetensors file %s has no mtime from cluster_cache file_metadata: %w", file.path, ErrSourceUnparseable)
	}

	lengthBytes, err := c.readRemoteRange(ctx, file, 0, 8, c.chunkSize)
	if err != nil {
		return nil, fmt.Errorf("read safetensors header length %s through cluster_cache: %w", file.path, err)
	}
	headerLength := binary.LittleEndian.Uint64(lengthBytes)
	if headerLength > maxClusterCacheSafetensorHeader {
		return nil, fmt.Errorf("safetensors header length %d in %s exceeds limit %d: %w", headerLength, file.path, maxClusterCacheSafetensorHeader, ErrSourceUnparseable)
	}
	if headerLength > file.size-8 {
		return nil, fmt.Errorf("safetensors header length %d in %s exceeds file size %d: %w", headerLength, file.path, file.size, ErrSourceUnparseable)
	}

	headerJSON, err := c.readRemoteRange(ctx, file, 8, headerLength, c.chunkSize)
	if err != nil {
		return nil, fmt.Errorf("read safetensors header %s through cluster_cache: %w", file.path, err)
	}
	return &SafetensorHeader{
		Path:         file.path,
		RelativePath: file.relativePath,
		SizeBytes:    uint64ToInt64(file.size),
		Version:      file.version,
		HeaderJSON:   headerJSON,
	}, nil
}

// readRemoteRange slices the requested byte range into chunk-aligned
// packed_byte_ranges_by_key specs and issues one POST to the gateway.
func (c *clusterCacheClient) readRemoteRange(ctx context.Context, file *clusterCacheMetadataFile, offset, length, chunkSize uint64) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	if chunkSize == 0 {
		return nil, fmt.Errorf("cluster_cache chunk_size is zero")
	}
	if file.mtime == nil {
		return nil, fmt.Errorf("cluster_cache file %s has no mtime; cannot build packed_byte_ranges spec", file.path)
	}
	if offset > file.size || length > file.size-offset {
		return nil, fmt.Errorf("range [%d,%d) exceeds %s size %d", offset, offset+length, file.path, file.size)
	}

	ranges := make([]packedByteRangeSpec, 0, 1)
	readOffset := offset
	remaining := length
	for remaining > 0 {
		chunkOffset := (readOffset / chunkSize) * chunkSize
		rangeOffset := readOffset - chunkOffset
		chunkLength := min(chunkSize, file.size-chunkOffset)
		rangeLength := min(remaining, chunkLength-rangeOffset)
		if rangeLength == 0 {
			return nil, fmt.Errorf("range for %s produced an empty cluster_cache chunk read", file.path)
		}
		ranges = append(ranges, packedByteRangeSpec{
			Path:        file.path,
			ChunkOffset: chunkOffset,
			MTime:       *file.mtime,
			ChunkLength: chunkLength,
			RangeOffset: rangeOffset,
			Length:      rangeLength,
		})
		readOffset += rangeLength
		remaining -= rangeLength
	}

	return c.packedByteRanges(ctx, ranges)
}

func (c *clusterCacheClient) packedByteRanges(ctx context.Context, ranges []packedByteRangeSpec) ([]byte, error) {
	var expectedBytes uint64
	for _, item := range ranges {
		expectedBytes += item.Length
	}

	body, err := json.Marshal(packedByteRangeBatchRequest{Ranges: ranges})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpointURL("/packed_byte_ranges_by_key", nil), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, endpointStatusError("packed_byte_ranges_by_key", response)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, int64(expectedBytes)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) != expectedBytes {
		return nil, fmt.Errorf("cluster_cache packed range response returned %d bytes, expected %d", len(data), expectedBytes)
	}
	return data, nil
}
