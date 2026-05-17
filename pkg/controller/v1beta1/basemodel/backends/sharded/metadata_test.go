package sharded

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/onsi/gomega"
)

func TestModelMetadataInputsFetchesHeaders(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	source := Source{URI: "s3://bucket/models/tiny"}
	configData := []byte(`{"model_type":"phi","architectures":["PhiForCausalLM"]}`)
	indexData := []byte(`{"weight_map":{"weight":"model-00001-of-00001.safetensors"}}`)
	safetensorsData := safetensorsHeaderForTest(`{"weight":{"dtype":"F32","shape":[2,3],"data_offsets":[0,24]}}`)
	payloadSize := int64(1024 * 1024)

	var inspectCalls []Source
	cacheClient := &stubClient{
		inspectSourceFunc: func(ctx context.Context, got Source) (*SourceInspection, error) {
			inspectCalls = append(inspectCalls, got)
			g.Expect(got).To(gomega.Equal(source))
			return &SourceInspection{
				Objects: []Object{
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
				Metadata: ModelMetadataInputs{
					ConfigJSON:           configData,
					SafetensorsIndexJSON: indexData,
					SafetensorHeaders: []SafetensorHeader{
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

	inspection, inputs, err := modelMetadataInputs(context.Background(), cacheClient, source)

	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(inspection.Objects).To(gomega.HaveLen(2))
	g.Expect(inputs).To(gomega.HaveLen(3))
	g.Expect(inputs[0].Path).To(gomega.Equal("config.json"))
	g.Expect(inputs[0].Data).To(gomega.Equal(configData))
	g.Expect(inputs[1].Path).To(gomega.Equal(defaultSafetensorsIndexFileName))
	g.Expect(inputs[1].Data).To(gomega.Equal(indexData))
	g.Expect(inputs[2].Path).To(gomega.Equal("model-00001-of-00001.safetensors"))
	g.Expect(inputs[2].Data).To(gomega.Equal(safetensorsData))
	g.Expect(inspectCalls).To(gomega.Equal([]Source{source}))
}

func TestMetadataInputsPreserveSubdirectorySafetensorsIndex(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	sourceURI := "s3://bucket/models/tiny"
	configData := []byte(`{"model_type":"phi","architectures":["PhiForCausalLM"]}`)
	indexData := []byte(`{"weight_map":{"weight":"model-00001-of-00001.safetensors"}}`)
	safetensorsData := safetensorsHeaderForTest(`{"weight":{"dtype":"F32","shape":[2,3],"data_offsets":[0,24]}}`)

	inputs, err := metadataInputsFromInspection(sourceURI, &SourceInspection{
		RootURI: sourceURI,
		Metadata: ModelMetadataInputs{
			ConfigJSON:           configData,
			ConfigPath:           "config.json",
			SafetensorsIndexJSON: indexData,
			SafetensorsIndexPath: "weights/model.safetensors.index.json",
			SafetensorHeaders: []SafetensorHeader{
				{
					Path:         sourceURI + "/weights/model-00001-of-00001.safetensors",
					RelativePath: "weights/model-00001-of-00001.safetensors",
					HeaderJSON:   safetensorsHeaderJSONForTest(safetensorsData),
				},
			},
		},
	})

	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(inputs).To(gomega.HaveLen(3))
	g.Expect(inputs[0].Path).To(gomega.Equal("config.json"))
	g.Expect(inputs[1].Path).To(gomega.Equal("weights/model.safetensors.index.json"))
	g.Expect(inputs[2].Path).To(gomega.Equal("weights/model-00001-of-00001.safetensors"))
}

func TestMetadataInputPathRejectsEscapingPath(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	_, ok, err := metadataInputPath("s3://bucket/models/tiny", "../config.json")

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
