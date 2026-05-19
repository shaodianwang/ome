package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"

	"github.com/ome-projects/ome/pkg/xet"
	"github.com/sgl-project/ome/pkg/afero"
	"github.com/sgl-project/ome/pkg/logging"
)

// hfDownloadAgentParams represents the parameters for dependency injection
type hfDownloadAgentParams struct {
	fx.In
	Logger logging.Interface `name:"another_log"`
}

// HFDownloadAgent implements the AgentModule interface for HuggingFace download agent
type HFDownloadAgent struct {
	client *xet.Client
	viper  *viper.Viper
	logger logging.Interface
}

// Name returns the name of the agent
func (h *HFDownloadAgent) Name() string {
	return "hf-download"
}

// ShortDescription returns a short description of the agent
func (h *HFDownloadAgent) ShortDescription() string {
	return "Run OME HuggingFace Download Agent"
}

// LongDescription returns a detailed description of the agent
func (h *HFDownloadAgent) LongDescription() string {
	return "OME Agent HuggingFace Download Agent downloads models from HuggingFace Hub using the xet client (Rust-backed libxet) with chunk-level deduplication, resume capability, and concurrent downloads."
}

// ConfigureCommand configures the agent command
func (h *HFDownloadAgent) ConfigureCommand(cmd *cobra.Command) {
	// Set the default action for this command
	cmd.Run = func(cmd *cobra.Command, args []string) {
		runAgentCommand(cmd, h, h.Start)
	}
}

// FxModules returns the fx modules needed by this agent
func (h *HFDownloadAgent) FxModules() []fx.Option {
	return []fx.Option{
		afero.Module,
		logging.Module,
		logging.ModuleNamed("another_log"),
		xet.Module, // xet module wires its own logger and reads viper config
		fx.Invoke(func(params hfDownloadAgentParams, client *xet.Client, v *viper.Viper) {
			h.client = client
			h.viper = v
			h.logger = params.Logger
		}),
	}
}

// Start starts the agent
func (h *HFDownloadAgent) Start() error {
	modelName := h.viper.GetString("model_name")
	localPath := h.viper.GetString("local_path")
	revision := h.viper.GetString("revision")
	repoType := h.viper.GetString("repo_type")

	if h.logger != nil {
		h.logger.Infof("🤗 Starting HuggingFace model download")
		h.logger.Infof("   Model: %s", modelName)
		h.logger.Infof("   Revision: %s (defaults to 'main' if empty)", revision)
		h.logger.Infof("   Target: %s", localPath)
		h.logger.Infof("   Repository Type: %s (defaults to 'model' if empty)", repoType)
	}

	if repoType == "" {
		repoType = xet.RepoTypeModel
	}
	if revision == "" {
		revision = "main"
	}

	req := &xet.SnapshotRequest{
		RepoID:   modelName,
		RepoType: repoType,
		Revision: revision,
		LocalDir: localPath,
	}

	downloadPath, err := h.client.DownloadSnapshotWithContext(context.Background(), req)
	if err != nil {
		if h.logger != nil {
			h.logger.Errorf("Failed to download model %s: %v", modelName, err)
		}
		return fmt.Errorf("model download failed: %w", err)
	}

	if h.logger != nil {
		h.logger.Infof("Successfully downloaded model %s", modelName)
		h.logger.Infof("Downloaded to: %s", downloadPath)
	}

	return nil
}

// NewHFDownloadAgent creates a new HuggingFace download agent
func NewHFDownloadAgent() *HFDownloadAgent {
	return &HFDownloadAgent{}
}
