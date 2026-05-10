package clustercache

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

	"github.com/sgl-project/ome/pkg/modelcache"
)

const (
	defaultConfigFileName            = "config.json"
	defaultModelIndexFileName        = "model_index.json"
	defaultSafetensorsIndexFileName  = "model.safetensors.index.json"
	maxClusterCacheMetadataFileBytes = 64 * 1024 * 1024
	maxClusterCacheSafetensorHeader  = 10 * 1024 * 1024
)

type ringInfoResponse struct {
	Nodes []string `json:"nodes"`
}

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

type indexedPackedRange struct {
	index int
	spec  packedByteRangeSpec
}

func (c *Client) inspectSourceFromFileMetadata(ctx context.Context, source modelcache.Source, metadata *fileMetadataResponse) (*modelcache.SourceInspection, error) {
	files := metadataFilesFromResponse(source, metadata)
	if len(files) == 0 {
		return nil, fmt.Errorf("cluster_cache file_metadata returned no files for model source")
	}

	chunkSize, err := c.metadataChunkSize(ctx, files)
	if err != nil {
		return nil, err
	}
	ring, err := c.ring(ctx)
	if err != nil {
		return nil, err
	}

	inputs, err := c.readMetadataInputs(ctx, files, chunkSize, ring)
	if err != nil {
		return nil, err
	}

	inspection := metadata.toModelCacheSourceInspection(source)
	inspection.Metadata = inputs
	return inspection, nil
}

func metadataFilesFromResponse(source modelcache.Source, response *fileMetadataResponse) []clusterCacheMetadataFile {
	if response == nil {
		return nil
	}

	root := firstNonEmpty(response.Root, source.URI)
	files := make([]clusterCacheMetadataFile, 0, len(response.Files))
	for _, entry := range response.Files {
		objectPath := inspectPath(root, entry.Path, "", entry.RelativePath)
		if objectPath == "" {
			continue
		}
		relativePath := cleanMetadataRelativePath(entry.RelativePath)
		if relativePath == "" {
			relativePath = relativePathFromRoot(root, objectPath)
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

func (c *Client) metadataChunkSize(ctx context.Context, files []clusterCacheMetadataFile) (uint64, error) {
	candidates := make([]*clusterCacheMetadataFile, 0, 3)
	for _, name := range []string{
		defaultConfigFileName,
		defaultModelIndexFileName,
	} {
		if file := selectMetadataFileByBase(files, name); file != nil {
			candidates = append(candidates, file)
		}
	}
	if len(candidates) == 0 {
		return 0, fmt.Errorf("cluster_cache source inspection requires %s or %s", defaultConfigFileName, defaultModelIndexFileName)
	}

	response, err := c.fetch(ctx, []string{candidates[0].path})
	if err != nil {
		return 0, fmt.Errorf("fetch cluster_cache metadata file for chunk sizing: %w", err)
	}
	for _, result := range response.Results {
		if result.Path != candidates[0].path {
			continue
		}
		if result.Status == "error" {
			return 0, fmt.Errorf("fetch cluster_cache metadata file for chunk sizing failed: %s", fetchResultError(result, "cluster_cache fetch failed"))
		}
		if result.TotalSize != nil {
			candidates[0].size = *result.TotalSize
		}
		if result.MTime != nil {
			mtime := *result.MTime
			candidates[0].mtime = &mtime
		}
		if result.ChunkSize == nil || *result.ChunkSize == 0 {
			return 0, fmt.Errorf("cluster_cache fetch did not return chunk_size for %s", candidates[0].path)
		}
		return *result.ChunkSize, nil
	}

	return 0, fmt.Errorf("cluster_cache fetch did not return metadata for %s", candidates[0].path)
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

func (c *Client) ring(ctx context.Context) (*clusterCacheRing, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpointURL("/ring_info", nil), nil)
	if err != nil {
		return nil, err
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, endpointStatusError("ring_info", response)
	}

	var parsed ringInfoResponse
	if err := json.NewDecoder(response.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode cluster_cache ring_info response: %w", err)
	}
	ring, err := newClusterCacheRing(parsed.Nodes)
	if err != nil {
		return nil, err
	}
	return ring, nil
}

func (c *Client) readMetadataInputs(ctx context.Context, files []clusterCacheMetadataFile, chunkSize uint64, ring *clusterCacheRing) (modelcache.ModelMetadataInputs, error) {
	inputs := modelcache.ModelMetadataInputs{}

	if file := selectMetadataFileByBase(files, defaultModelIndexFileName); file != nil {
		data, err := c.readWholeMetadataFile(ctx, file, chunkSize, ring)
		if err != nil {
			return inputs, fmt.Errorf("read %s through cluster_cache: %w", defaultModelIndexFileName, err)
		}
		inputs.ModelIndexJSON = data
		inputs.ModelIndexPath = file.relativePath
	}
	if file := selectMetadataFileByBase(files, defaultConfigFileName); file != nil {
		data, err := c.readWholeMetadataFile(ctx, file, chunkSize, ring)
		if err != nil {
			return inputs, fmt.Errorf("read %s through cluster_cache: %w", defaultConfigFileName, err)
		}
		inputs.ConfigJSON = data
		inputs.ConfigPath = file.relativePath
	}
	if file := selectMetadataFileByBase(files, defaultSafetensorsIndexFileName); file != nil {
		data, err := c.readWholeMetadataFile(ctx, file, chunkSize, ring)
		if err != nil {
			return inputs, fmt.Errorf("read %s through cluster_cache: %w", defaultSafetensorsIndexFileName, err)
		}
		inputs.SafetensorsIndexJSON = data
		inputs.SafetensorsIndexPath = file.relativePath
	}

	safetensorFiles, err := safetensorMetadataFiles(files, selectMetadataFileByBase(files, defaultSafetensorsIndexFileName), inputs.SafetensorsIndexJSON)
	if err != nil {
		return inputs, err
	}
	for i := range safetensorFiles {
		header, err := c.readSafetensorHeader(ctx, &safetensorFiles[i], chunkSize, ring)
		if err != nil {
			return inputs, err
		}
		inputs.SafetensorHeaders = append(inputs.SafetensorHeaders, *header)
	}
	return inputs, nil
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
				return nil, fmt.Errorf("safetensors shard %q referenced by %s was not listed by cluster_cache", ref, defaultSafetensorsIndexFileName)
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
		return nil, fmt.Errorf("weight_map is empty")
	}

	seen := map[string]struct{}{}
	shards := make([]string, 0, len(index.WeightMap))
	indexDir := path.Dir(cleanMetadataRelativePath(indexPath))
	for _, shard := range index.WeightMap {
		shard = cleanMetadataRelativePath(shard)
		if shard == "" {
			return nil, fmt.Errorf("weight_map includes an empty or invalid shard path")
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

func (c *Client) readWholeMetadataFile(ctx context.Context, file *clusterCacheMetadataFile, chunkSize uint64, ring *clusterCacheRing) ([]byte, error) {
	if file.size > maxClusterCacheMetadataFileBytes {
		return nil, fmt.Errorf("metadata file %s is %d bytes, above limit %d", file.path, file.size, maxClusterCacheMetadataFileBytes)
	}
	return c.readRemoteRange(ctx, file, 0, file.size, chunkSize, ring)
}

func (c *Client) readSafetensorHeader(ctx context.Context, file *clusterCacheMetadataFile, chunkSize uint64, ring *clusterCacheRing) (*modelcache.SafetensorHeader, error) {
	if file.size < 8 {
		return nil, fmt.Errorf("safetensors file %s is too small to contain a header length", file.path)
	}

	lengthBytes, err := c.readRemoteRange(ctx, file, 0, 8, chunkSize, ring)
	if err != nil {
		return nil, fmt.Errorf("read safetensors header length %s through cluster_cache: %w", file.path, err)
	}
	headerLength := binary.LittleEndian.Uint64(lengthBytes)
	if headerLength > maxClusterCacheSafetensorHeader {
		return nil, fmt.Errorf("safetensors header length %d in %s exceeds limit %d", headerLength, file.path, maxClusterCacheSafetensorHeader)
	}
	if headerLength > file.size-8 {
		return nil, fmt.Errorf("safetensors header length %d in %s exceeds file size %d", headerLength, file.path, file.size)
	}

	headerJSON, err := c.readRemoteRange(ctx, file, 8, headerLength, chunkSize, ring)
	if err != nil {
		return nil, fmt.Errorf("read safetensors header %s through cluster_cache: %w", file.path, err)
	}
	return &modelcache.SafetensorHeader{
		Path:         file.path,
		RelativePath: file.relativePath,
		SizeBytes:    uint64ToInt64(file.size),
		Version:      file.version,
		HeaderJSON:   headerJSON,
	}, nil
}

func (c *Client) readRemoteRange(ctx context.Context, file *clusterCacheMetadataFile, offset, length, chunkSize uint64, ring *clusterCacheRing) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	if chunkSize == 0 {
		return nil, fmt.Errorf("cluster_cache chunk_size is zero")
	}
	if offset > file.size || length > file.size-offset {
		return nil, fmt.Errorf("range [%d,%d) exceeds %s size %d", offset, offset+length, file.path, file.size)
	}
	if err := c.ensureMetadataFileMTime(ctx, file); err != nil {
		return nil, err
	}

	ranges := make([]indexedPackedRange, 0, 1)
	readOffset := offset
	remaining := length
	for remaining > 0 {
		chunkOffset := (readOffset / chunkSize) * chunkSize
		rangeOffset := readOffset - chunkOffset
		chunkLength := minUint64(chunkSize, file.size-chunkOffset)
		rangeLength := minUint64(remaining, chunkLength-rangeOffset)
		if rangeLength == 0 {
			return nil, fmt.Errorf("range for %s produced an empty cluster_cache chunk read", file.path)
		}
		ranges = append(ranges, indexedPackedRange{
			index: len(ranges),
			spec: packedByteRangeSpec{
				Path:        file.path,
				ChunkOffset: chunkOffset,
				MTime:       *file.mtime,
				ChunkLength: chunkLength,
				RangeOffset: rangeOffset,
				Length:      rangeLength,
			},
		})
		readOffset += rangeLength
		remaining -= rangeLength
	}

	pieces := make([][]byte, len(ranges))
	grouped := map[string][]indexedPackedRange{}
	for _, item := range ranges {
		owner, err := ring.lookup(clusterCacheChunkKey{
			path:   file.path,
			offset: item.spec.ChunkOffset,
			mtime:  item.spec.MTime,
		})
		if err != nil {
			return nil, err
		}
		grouped[owner] = append(grouped[owner], item)
	}

	for owner, ownerRanges := range grouped {
		body, err := c.packedByteRanges(ctx, owner, ownerRanges)
		if err != nil {
			if err := c.readPackedRangesWithFallback(ctx, ring, ownerRanges, pieces); err != nil {
				return nil, err
			}
			continue
		}
		bodyOffset := 0
		for _, item := range ownerRanges {
			end := bodyOffset + int(item.spec.Length)
			if end > len(body) {
				return nil, fmt.Errorf("cluster_cache packed range response from %s was shorter than expected", owner)
			}
			pieces[item.index] = append([]byte(nil), body[bodyOffset:end]...)
			bodyOffset = end
		}
		if bodyOffset != len(body) {
			return nil, fmt.Errorf("cluster_cache packed range response from %s was longer than expected", owner)
		}
	}

	return bytes.Join(pieces, nil), nil
}

func (c *Client) readPackedRangesWithFallback(ctx context.Context, ring *clusterCacheRing, ranges []indexedPackedRange, pieces [][]byte) error {
	for _, item := range ranges {
		candidates := ring.lookupCandidates(clusterCacheChunkKey{
			path:   item.spec.Path,
			offset: item.spec.ChunkOffset,
			mtime:  item.spec.MTime,
		})
		var errors []string
		for _, candidate := range candidates {
			body, err := c.packedByteRanges(ctx, candidate, []indexedPackedRange{item})
			if err == nil {
				pieces[item.index] = body
				break
			}
			errors = append(errors, fmt.Sprintf("%s: %v", candidate, err))
		}
		if pieces[item.index] == nil {
			return fmt.Errorf("cluster_cache packed range read failed for %s chunk %d: %s", item.spec.Path, item.spec.ChunkOffset, strings.Join(errors, "; "))
		}
	}
	return nil
}

func (c *Client) ensureMetadataFileMTime(ctx context.Context, file *clusterCacheMetadataFile) error {
	if file.mtime != nil {
		return nil
	}
	head, err := c.head(ctx, file.path)
	if err != nil {
		return fmt.Errorf("head cluster_cache metadata file %s: %w", file.path, err)
	}
	file.size = head.Size
	file.mtime = &head.MTime
	if file.version == "" {
		file.version = strconvFormatUint(head.MTime)
	}
	return nil
}

func (c *Client) head(ctx context.Context, path string) (*headResponse, error) {
	query := url.Values{}
	query.Set("path", path)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpointURL("/head", query), nil)
	if err != nil {
		return nil, err
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, endpointStatusError("head", response)
	}

	var parsed headResponse
	if err := json.NewDecoder(response.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode cluster_cache head response: %w", err)
	}
	return &parsed, nil
}

func (c *Client) packedByteRanges(ctx context.Context, owner string, ranges []indexedPackedRange) ([]byte, error) {
	specs := make([]packedByteRangeSpec, 0, len(ranges))
	var expectedBytes uint64
	for _, item := range ranges {
		specs = append(specs, item.spec)
		expectedBytes += item.spec.Length
	}

	body, err := json.Marshal(packedByteRangeBatchRequest{Ranges: specs})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.nodeEndpointURL(owner, "/packed_byte_ranges_by_key", nil), bytes.NewReader(body))
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
		return nil, fmt.Errorf("cluster_cache packed range response from %s returned %d bytes, expected %d", owner, len(data), expectedBytes)
	}
	return data, nil
}

func (c *Client) nodeEndpointURL(node, endpointPath string, query url.Values) string {
	endpoint := *c.endpoint
	endpoint.Host = node
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + endpointPath
	endpoint.RawQuery = query.Encode()
	return endpoint.String()
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func strconvFormatUint(value uint64) string {
	return fmt.Sprintf("%d", value)
}
