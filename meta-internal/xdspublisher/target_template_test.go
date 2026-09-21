//go:build meta_internal

package xdspublisher

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const conventionalTargetTemplate = `gke_llm_tpu_{{replaceAll "-" "_" .Service}}_{{replaceAll "-" "_" (mustTrimSuffix "-tpu" (mustTrimPrefix "msl-gcp-" .Cluster))}}`

func TestTargetTemplateBuildsConfiguredLogicalClusterName(t *testing.T) {
	targetTemplate, err := parseTargetTemplate(conventionalTargetTemplate)
	require.NoError(t, err)

	var rendered bytes.Buffer
	require.NoError(t, targetTemplate.Execute(&rendered, targetTemplateData{
		Service: "avocado-t5-reasoning-api-proxy-rc",
		Cluster: "msl-gcp-usc1-1-tpu",
	}))
	assert.Equal(t, "gke_llm_tpu_avocado_t5_reasoning_api_proxy_rc_usc1_1", rendered.String())
}

func TestTargetTemplateRequiredAffixesFailClosed(t *testing.T) {
	tests := []struct {
		name    string
		service string
		cluster string
		want    string
	}{
		{name: "unexpected prefix", service: "model", cluster: "other-usc1-1-tpu", want: "must start"},
		{name: "unexpected suffix", service: "model", cluster: "msl-gcp-usc1-1", want: "must end"},
		{name: "empty prefix remainder", service: "model", cluster: "msl-gcp-", want: "contain a value after"},
		{name: "empty suffix remainder", service: "model", cluster: "msl-gcp--tpu", want: "contain a value before"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targetTemplate, err := parseTargetTemplate(conventionalTargetTemplate)
			require.NoError(t, err)

			var rendered bytes.Buffer
			err = targetTemplate.Execute(&rendered, targetTemplateData{
				Service: tt.service,
				Cluster: tt.cluster,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestTargetTemplateRejectsEmptyFunctionArguments(t *testing.T) {
	for _, templateText := range []string{
		`{{replaceAll "" "_" .Service}}`,
		`{{mustTrimPrefix "" .Cluster}}`,
		`{{mustTrimSuffix "" .Cluster}}`,
	} {
		targetTemplate, err := parseTargetTemplate(templateText)
		require.NoError(t, err)

		var rendered bytes.Buffer
		err = targetTemplate.Execute(&rendered, targetTemplateData{Service: "model", Cluster: "cluster"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must not be empty")
	}
}
