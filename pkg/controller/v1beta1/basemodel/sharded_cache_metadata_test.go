package basemodel

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/onsi/gomega"

	"github.com/sgl-project/ome/pkg/modelcache"
	modelcachefake "github.com/sgl-project/ome/pkg/modelcache/fake"
)

func TestShardedModelMetadataInputsFetchesHeaders(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	source := modelcache.Source{URI: "s3://bucket/models/tiny"}
	configData := []byte(`{"model_type":"phi","architectures":["PhiForCausalLM"]}`)
	indexData := []byte(`{"weight_map":{"weight":"model-00001-of-00001.safetensors"}}`)
	safetensorsData := safetensorsHeaderForTest(`{"weight":{"dtype":"F32","shape":[2,3],"data_offsets":[0,24]}}`)
	payloadSize := int64(1024 * 1024)

	cacheClient := &modelcachefake.Client{
		InspectSourceFunc: func(ctx context.Context, got modelcache.Source) (*modelcache.SourceInspection, error) {
			g.Expect(got).To(gomega.Equal(source))
			return &modelcache.SourceInspection{
				Objects: []modelcache.Object{
					{
						Source:    source,
						Path:      "s3://bucket/models/tiny/config.json",
						SizeBytes: int64(len(configData)),
					},
					{
						Source:    source,
						Path:      "s3://bucket/models/tiny/model-00001-of-00001.safetensors",
						SizeBytes: int64(len(safetensorsData)) + payloadSize,
					},
				},
				Metadata: modelcache.ModelMetadataInputs{
					ConfigJSON:           configData,
					SafetensorsIndexJSON: indexData,
					SafetensorHeaders: []modelcache.SafetensorHeader{
						{
							Path:       "s3://bucket/models/tiny/model-00001-of-00001.safetensors",
							SizeBytes:  int64(len(safetensorsData)) + payloadSize,
							HeaderJSON: safetensorsHeaderJSONForTest(safetensorsData),
						},
					},
				},
			}, nil
		},
	}

	inspection, inputs, err := shardedModelMetadataInputs(context.Background(), cacheClient, source)

	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(inspection.Objects).To(gomega.HaveLen(2))
	g.Expect(inputs).To(gomega.HaveLen(3))
	g.Expect(inputs[0].Path).To(gomega.Equal("config.json"))
	g.Expect(inputs[0].Data).To(gomega.Equal(configData))
	g.Expect(inputs[1].Path).To(gomega.Equal(defaultSafetensorsIndexFileName))
	g.Expect(inputs[1].Data).To(gomega.Equal(indexData))
	g.Expect(inputs[2].Path).To(gomega.Equal("model-00001-of-00001.safetensors"))
	g.Expect(inputs[2].Data).To(gomega.Equal(safetensorsData))
	g.Expect(cacheClient.InspectSourceCalls()).To(gomega.Equal([]modelcache.Source{source}))
}

func TestShardedPrewarmObjectRefsDefaultsSource(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	source := modelcache.Source{URI: "s3://bucket/models/tiny"}
	inspection := &modelcache.SourceInspection{
		Objects: []modelcache.Object{
			{Path: "s3://bucket/models/tiny/config.json"},
			{Path: ""},
		},
	}

	refs := shardedPrewarmObjectRefs(source, inspection)

	g.Expect(refs).To(gomega.Equal([]modelcache.ObjectRef{{
		Source: source,
		Path:   "s3://bucket/models/tiny/config.json",
	}}))
}

func TestShardedMetadataInputPathRejectsEscapingPath(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	_, ok, err := shardedMetadataInputPath("s3://bucket/models/tiny", "../config.json")

	g.Expect(ok).To(gomega.BeFalse())
	g.Expect(err).To(gomega.HaveOccurred())
}

func safetensorsHeaderForTest(header string) []byte {
	headerBytes := []byte(header)
	var buffer bytes.Buffer
	lengthBytes := make([]byte, safetensorsHeaderLengthFieldBytes)
	binary.LittleEndian.PutUint64(lengthBytes, uint64(len(headerBytes)))
	buffer.Write(lengthBytes)
	buffer.Write(headerBytes)
	return buffer.Bytes()
}

func safetensorsHeaderJSONForTest(data []byte) []byte {
	length := binary.LittleEndian.Uint64(data[:safetensorsHeaderLengthFieldBytes])
	return data[safetensorsHeaderLengthFieldBytes : safetensorsHeaderLengthFieldBytes+int(length)]
}
