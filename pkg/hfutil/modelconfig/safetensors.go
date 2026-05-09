package modelconfig

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const maxSafetensorsHeaderSize = 10 * 1024 * 1024

// SafetensorsFileInput provides a safetensors-related file from a non-filesystem source.
// Data for .safetensors files only needs to contain the 8-byte header length followed by
// the JSON header bytes; tensor payload bytes are not required for metadata parsing.
type SafetensorsFileInput struct {
	Path string
	Data []byte
}

// FindAndParseSafetensors looks for a safetensors file in the same directory as the config file
// and parses it to count the total number of parameters
func FindAndParseSafetensors(configPath string) (int64, error) {
	if configPath == "" {
		return 0, fmt.Errorf("config path cannot be empty")
	}

	dir := filepath.Dir(configPath)
	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("failed to list directory '%s': %w", dir, err)
	}

	// Look for index.json file first, which might point to sharded safetensors
	indexPath := filepath.Join(dir, "model.safetensors.index.json")
	if _, err := os.Stat(indexPath); err == nil {
		count, err := ParseSafetensorsIndex(indexPath)
		if err != nil {
			return 0, fmt.Errorf("failed to parse safetensors index '%s': %w", indexPath, err)
		}
		return count, nil
	}

	// If no index file, process all safetensors files and sum their parameters
	var totalParams int64
	safetensorsFound := false

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".safetensors") {
			fullPath := filepath.Join(dir, f.Name())
			params, err := ParseSafetensors(fullPath)
			if err != nil {
				return 0, fmt.Errorf("failed to parse safetensors file '%s': %w", fullPath, err)
			}

			// Check for overflow when adding
			if totalParams > math.MaxInt64-params {
				return 0, fmt.Errorf("parameter count overflow when processing '%s': total would exceed maximum value", fullPath)
			}

			totalParams += params
			safetensorsFound = true
		}
	}

	if !safetensorsFound {
		return 0, fmt.Errorf("no .safetensors files found in directory '%s'", dir)
	}

	return totalParams, nil
}

// FindAndParseSafetensorsFiles parses safetensors metadata from an explicit set of files.
// It supports either model.safetensors.index.json plus referenced shard headers, or
// all .safetensors files in the set when no index is present.
func FindAndParseSafetensorsFiles(files []SafetensorsFileInput) (int64, error) {
	if len(files) == 0 {
		return 0, fmt.Errorf("safetensors file input set is empty")
	}

	byPath := make(map[string][]byte, len(files))
	var indexPaths []string
	var safetensorsPaths []string
	for _, file := range files {
		filePath := cleanSafetensorsInputPath(file.Path)
		if filePath == "" {
			continue
		}
		byPath[filePath] = file.Data
		switch path.Base(filePath) {
		case "model.safetensors.index.json":
			indexPaths = append(indexPaths, filePath)
		default:
			if strings.HasSuffix(path.Base(filePath), ".safetensors") {
				safetensorsPaths = append(safetensorsPaths, filePath)
			}
		}
	}

	sort.Strings(indexPaths)
	if len(indexPaths) > 0 {
		indexPath := indexPaths[0]
		indexDir := path.Dir(indexPath)
		return parseSafetensorsIndexData(byPath[indexPath], indexPath, func(shard string) (int64, error) {
			shardPath := cleanSafetensorsInputPath(shard)
			if shardPath == "" {
				return 0, fmt.Errorf("empty shard filename found in index '%s'", indexPath)
			}
			if indexDir != "." {
				shardPath = path.Join(indexDir, shardPath)
			}
			data, ok := byPath[shardPath]
			if !ok {
				return 0, fmt.Errorf("shard '%s' referenced in index '%s' was not provided", shardPath, indexPath)
			}
			return ParseSafetensorsBytes(data, shardPath)
		})
	}

	sort.Strings(safetensorsPaths)
	if len(safetensorsPaths) == 0 {
		return 0, fmt.Errorf("no .safetensors files found in input set")
	}

	var totalParams int64
	for _, safetensorsPath := range safetensorsPaths {
		params, err := ParseSafetensorsBytes(byPath[safetensorsPath], safetensorsPath)
		if err != nil {
			return 0, fmt.Errorf("failed to parse safetensors file '%s': %w", safetensorsPath, err)
		}
		if totalParams > math.MaxInt64-params {
			return 0, fmt.Errorf("parameter count overflow when processing '%s': total would exceed maximum value", safetensorsPath)
		}
		totalParams += params
	}
	return totalParams, nil
}

// ParseSafetensors parses a single safetensors file and counts parameters
func ParseSafetensors(path string) (int64, error) {
	if path == "" {
		return 0, fmt.Errorf("safetensors file path cannot be empty")
	}

	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("failed to open safetensors file '%s': %w", path, err)
	}
	defer file.Close()

	return ParseSafetensorsReader(file, path)
}

// ParseSafetensorsBytes parses safetensors metadata from bytes containing the
// 8-byte header length followed by the safetensors JSON header.
func ParseSafetensorsBytes(data []byte, label string) (int64, error) {
	return ParseSafetensorsReader(bytes.NewReader(data), label)
}

// ParseSafetensorsReader parses safetensors metadata from a reader.
func ParseSafetensorsReader(reader io.Reader, label string) (int64, error) {
	if label == "" {
		label = "safetensors input"
	}

	headerLenBuf := make([]byte, 8)
	if _, err := io.ReadFull(reader, headerLenBuf); err != nil {
		return 0, fmt.Errorf("failed to read header length from '%s': %w", label, err)
	}
	headerLen := binary.LittleEndian.Uint64(headerLenBuf)

	// Sanity check for header length to prevent excessive memory allocation
	if headerLen > maxSafetensorsHeaderSize {
		return 0, fmt.Errorf("header length %d in '%s' exceeds maximum allowed size of %d bytes",
			headerLen, label, maxSafetensorsHeaderSize)
	}

	headerBytes := make([]byte, headerLen)
	if _, err := io.ReadFull(reader, headerBytes); err != nil {
		return 0, fmt.Errorf("failed to read JSON header from '%s': %w", label, err)
	}

	return ParseSafetensorsHeader(headerBytes, label)
}

// ParseSafetensorsHeader parses a safetensors JSON header and counts parameters.
func ParseSafetensorsHeader(headerBytes []byte, label string) (int64, error) {
	if label == "" {
		label = "safetensors input"
	}

	var raw map[string]struct {
		Shape       []int64  `json:"shape"`
		Dtype       string   `json:"dtype"`
		DataOffsets [2]int64 `json:"data_offsets"`
	}

	if err := json.Unmarshal(headerBytes, &raw); err != nil {
		return 0, fmt.Errorf("failed to parse JSON header from '%s': %w", label, err)
	}

	var total int64 = 0
	for tensorName, tensor := range raw {
		// Skip metadata tensors which may have empty shapes
		if tensorName == "__metadata__" {
			continue
		}

		if len(tensor.Shape) == 0 {
			// Skip tensors with empty shapes (like scalars or metadata)
			continue
		}

		count := int64(1)
		for i, dim := range tensor.Shape {
			if dim <= 0 {
				return 0, fmt.Errorf("tensor '%s' in '%s' has invalid dimension %d at index %d",
					tensorName, label, dim, i)
			}

			// Check for overflow before multiplication
			if count > 0 && dim > math.MaxInt64/count {
				return 0, fmt.Errorf("dimension overflow for tensor '%s' in '%s': multiplication would exceed maximum value",
					tensorName, label)
			}
			count *= dim
		}

		// Check for overflow when adding to total
		if total > math.MaxInt64-count {
			return 0, fmt.Errorf("parameter count overflow in '%s': total would exceed maximum value", label)
		}
		total += count
	}

	return total, nil
}

// ParseSafetensorsIndex parses a model.safetensors.index.json file for sharded models
func ParseSafetensorsIndex(indexPath string) (int64, error) {
	if indexPath == "" {
		return 0, fmt.Errorf("index path cannot be empty")
	}

	data, err := os.ReadFile(indexPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read safetensors index file '%s': %w", indexPath, err)
	}

	dir := filepath.Dir(indexPath)
	return parseSafetensorsIndexData(data, indexPath, func(shard string) (int64, error) {
		shardPath := filepath.Join(dir, shard)
		return ParseSafetensors(shardPath)
	})
}

func parseSafetensorsIndexData(data []byte, indexPath string, parseShard func(string) (int64, error)) (int64, error) {
	var index struct {
		WeightMap map[string]string `json:"weight_map"`
	}

	if err := json.Unmarshal(data, &index); err != nil {
		return 0, fmt.Errorf("failed to parse safetensors index JSON from '%s': %w", indexPath, err)
	}

	if len(index.WeightMap) == 0 {
		return 0, fmt.Errorf("no weight mappings found in safetensors index '%s'", indexPath)
	}

	shardFiles := make(map[string]bool)
	for _, shard := range index.WeightMap {
		if shard == "" {
			return 0, fmt.Errorf("empty shard filename found in index '%s'", indexPath)
		}
		shardFiles[shard] = true
	}

	shards := make([]string, 0, len(shardFiles))
	for shard := range shardFiles {
		shards = append(shards, shard)
	}
	sort.Strings(shards)

	var total int64
	for _, shard := range shards {
		count, err := parseShard(shard)
		if err != nil {
			return 0, fmt.Errorf("failed to parse shard '%s' referenced in index '%s': %w", shard, indexPath, err)
		}
		if total > math.MaxInt64-count {
			return 0, fmt.Errorf("parameter count overflow when processing shards from '%s': total would exceed maximum value", indexPath)
		}
		total += count
	}

	return total, nil
}

func cleanSafetensorsInputPath(filePath string) string {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return ""
	}
	filePath = strings.TrimLeft(filepath.ToSlash(filePath), "/")
	return path.Clean(filePath)
}
