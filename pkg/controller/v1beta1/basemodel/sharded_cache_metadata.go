package basemodel

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/sgl-project/ome/pkg/modelcache"
	"github.com/sgl-project/ome/pkg/modelparser"
)

const (
	maxShardedSafetensorsHeaderBytes  = 10 * 1024 * 1024
	safetensorsHeaderLengthFieldBytes = 8
	defaultSafetensorsIndexFileName   = "model.safetensors.index.json"
)

func shardedModelMetadataInputs(ctx context.Context, cacheClient modelcache.Client, source modelcache.Source) (*modelcache.SourceInspection, []modelparser.ModelConfigFileInput, error) {
	if cacheClient == nil {
		return nil, nil, fmt.Errorf("model cache client is nil")
	}

	inspection, err := cacheClient.InspectSource(ctx, source)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect model cache source for metadata: %w", err)
	}
	inputs, err := shardedMetadataInputsFromInspection(source.URI, inspection)
	if err != nil {
		return inspection, nil, err
	}
	return inspection, inputs, nil
}

func shardedMetadataInputsFromInspection(sourceURI string, inspection *modelcache.SourceInspection) ([]modelparser.ModelConfigFileInput, error) {
	if inspection == nil {
		return nil, fmt.Errorf("model cache source inspection returned nil")
	}

	metadata := inspection.Metadata
	inputs := make([]modelparser.ModelConfigFileInput, 0, 2+len(metadata.SafetensorHeaders))
	if len(metadata.ModelIndexJSON) > 0 {
		inputs = append(inputs, modelparser.ModelConfigFileInput{
			Path: pathOrDefault(metadata.ModelIndexPath, modelparser.DefaultModelIndexFileName),
			Data: metadata.ModelIndexJSON,
		})
	}
	if len(metadata.ConfigJSON) > 0 {
		inputs = append(inputs, modelparser.ModelConfigFileInput{
			Path: pathOrDefault(metadata.ConfigPath, modelparser.DefaultConfigFileName),
			Data: metadata.ConfigJSON,
		})
	}
	if len(metadata.SafetensorsIndexJSON) > 0 {
		inputs = append(inputs, modelparser.ModelConfigFileInput{
			Path: pathOrDefault(metadata.SafetensorsIndexPath, defaultSafetensorsIndexFileName),
			Data: metadata.SafetensorsIndexJSON,
		})
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("model cache source inspection did not include %s or %s", modelparser.DefaultModelIndexFileName, modelparser.DefaultConfigFileName)
	}

	for _, header := range metadata.SafetensorHeaders {
		inputPath, ok, err := shardedMetadataHeaderInputPath(sourceURI, header)
		if err != nil {
			return nil, err
		}
		if !ok || !strings.HasSuffix(path.Base(inputPath), ".safetensors") {
			continue
		}
		data, err := safetensorsHeaderInputData(header.HeaderJSON)
		if err != nil {
			return nil, fmt.Errorf("prepare safetensors header %s: %w", header.Path, err)
		}
		inputs = append(inputs, modelparser.ModelConfigFileInput{
			Path: inputPath,
			Data: data,
		})
	}
	return inputs, nil
}

func pathOrDefault(value, defaultValue string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return defaultValue
}

func safetensorsHeaderInputData(headerJSON []byte) ([]byte, error) {
	if len(headerJSON) == 0 {
		return nil, fmt.Errorf("safetensors header JSON is empty")
	}
	if len(headerJSON) > maxShardedSafetensorsHeaderBytes {
		return nil, fmt.Errorf("safetensors header length %d exceeds metadata limit %d", len(headerJSON), maxShardedSafetensorsHeaderBytes)
	}

	data := make([]byte, safetensorsHeaderLengthFieldBytes+len(headerJSON))
	binary.LittleEndian.PutUint64(data[:safetensorsHeaderLengthFieldBytes], uint64(len(headerJSON)))
	copy(data[safetensorsHeaderLengthFieldBytes:], headerJSON)
	return data, nil
}

func shardedPrewarmObjectRefs(source modelcache.Source, inspection *modelcache.SourceInspection) []modelcache.ObjectRef {
	if inspection == nil {
		return nil
	}

	refs := make([]modelcache.ObjectRef, 0, len(inspection.Objects))
	for _, object := range inspection.Objects {
		objectPath := strings.TrimSpace(object.Path)
		if objectPath == "" {
			continue
		}
		objectSource := object.Source
		if objectSource.URI == "" {
			objectSource = source
		}
		refs = append(refs, modelcache.ObjectRef{
			Source: objectSource,
			Path:   objectPath,
		})
	}
	return refs
}

func shardedMetadataInputPath(sourceURI, objectPath string) (string, bool, error) {
	objectPath = strings.TrimSpace(filepath.ToSlash(objectPath))
	if objectPath == "" {
		return "", false, nil
	}

	sourceURI = strings.TrimRight(strings.TrimSpace(filepath.ToSlash(sourceURI)), "/")
	if sourceURI != "" {
		if objectPath == sourceURI {
			return "", false, nil
		}
		if strings.HasPrefix(objectPath, sourceURI+"/") {
			objectPath = strings.TrimPrefix(objectPath, sourceURI+"/")
		}
	}

	if parsed, err := url.Parse(objectPath); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		objectPath = strings.TrimLeft(parsed.Path, "/")
	}
	objectPath = strings.TrimLeft(objectPath, "/")
	objectPath = path.Clean(objectPath)
	if objectPath == "." || objectPath == "" {
		return "", false, nil
	}
	if objectPath == ".." || strings.HasPrefix(objectPath, "../") {
		return "", false, fmt.Errorf("model cache object path must stay under model root: %q", objectPath)
	}
	return objectPath, true, nil
}

func shardedMetadataHeaderInputPath(sourceURI string, header modelcache.SafetensorHeader) (string, bool, error) {
	if strings.TrimSpace(header.RelativePath) != "" {
		return shardedMetadataInputPath("", header.RelativePath)
	}
	return shardedMetadataInputPath(sourceURI, header.Path)
}
