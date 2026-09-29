//go:build meta_internal

// Command ome-bot serves the OME Slack bot's HTTP API. See package omebot.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ome-projects/ome/meta-internal/omebot"
)

func main() {
	configPath := flag.String("config", "", "path to the YAML configuration (required)")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(*configPath, log); err != nil {
		log.Error("exit", "error", err.Error())
		os.Exit(1)
	}
}

func run(configPath string, log *slog.Logger) error {
	if configPath == "" {
		return errors.New("--config is required")
	}
	cfg, err := omebot.Load(configPath)
	if err != nil {
		return err
	}
	paths, err := omebot.KubeconfigPaths(cfg)
	if err != nil {
		return err
	}
	verifier := &omebot.Verifier{Mode: cfg.Auth.Mode}
	if cfg.Auth.Mode == omebot.AuthModeHMAC {
		secret, err := os.ReadFile(cfg.Auth.SecretFile)
		if err != nil {
			return fmt.Errorf("read auth secret: %w", err)
		}
		verifier.Secret = []byte(strings.TrimSpace(string(secret)))
		if len(verifier.Secret) == 0 {
			return errors.New("auth secret is empty")
		}
		verifier.MaxSkew = time.Duration(cfg.Auth.MaxSkewSeconds) * time.Second
	}
	prober := &omebot.Prober{Config: cfg, Paths: paths, Timeout: time.Duration(cfg.Timeouts.ProbeSeconds) * time.Second}
	release, err := omebot.NewRelease(cfg, prober)
	if err != nil {
		return err
	}
	var docs *omebot.Docs
	if cfg.Docs != nil {
		docs = &omebot.Docs{Config: *cfg.Docs, Limit: cfg.OutputLimitBytes}
	}
	srv := &omebot.Server{
		Config: cfg,
		Paths:  paths,
		Runner: &omebot.Runner{
			Bin:     cfg.KubectlOmePath,
			Timeout: time.Duration(cfg.Timeouts.CommandSeconds) * time.Second,
			Limit:   cfg.OutputLimitBytes,
			Env:     cfg.Env,
		},
		Prober:   prober,
		Verifier: verifier,
		Log:      log,
		Tools:    omebot.NewTools(cfg.Tools, docs, cfg.OutputLimitBytes),
		Release:  release,
	}
	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: time.Duration(cfg.Timeouts.ReadHeaderSeconds) * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() { errCh <- httpSrv.ListenAndServe() }()
	srv.SetReady(true)
	log.Info("listening", "addr", cfg.Listen, "clusters", cfg.ClusterNames(), "auth", cfg.Auth.Mode,
		"tools", srv.Tools.Names(), "release", release != nil)
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Timeouts.ShutdownSeconds)*time.Second)
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
