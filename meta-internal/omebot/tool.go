//go:build meta_internal

package omebot

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"
)

// ToolResult is one tool invocation as returned by POST /v1/tool.
type ToolResult struct {
	Tool      string `json:"tool"`
	ExitCode  int    `json:"exitCode"`
	Output    string `json:"output"`
	Truncated bool   `json:"truncated"`
	Error     string `json:"error,omitempty"`
}

// Tools runs registry tools and the built-in docs tool.
type Tools struct {
	byName map[string]ToolConfig
	docs   *Docs
	limit  int
}

// NewTools indexes a validated registry. docs may be nil.
func NewTools(cfgs []ToolConfig, docs *Docs, limit int) *Tools {
	t := &Tools{byName: make(map[string]ToolConfig, len(cfgs)), docs: docs, limit: limit}
	for _, c := range cfgs {
		t.byName[c.Name] = c
	}
	return t
}

// Names lists every callable tool, sorted.
func (t *Tools) Names() []string {
	names := make([]string, 0, len(t.byName)+1)
	for n := range t.byName {
		names = append(names, n)
	}
	if t.docs != nil {
		names = append(names, DocsToolName)
	}
	slices.Sort(names)
	return names
}

// Run executes one call. A non-empty class means the call was refused or
// timed out; a tool that ran and failed has an empty class and a non-zero
// exit code.
func (t *Tools) Run(ctx context.Context, name string, args []string) (ToolResult, string) {
	for _, a := range args {
		if hasControl(a) {
			return ToolResult{Tool: name, ExitCode: -1, Error: "control character in argument"}, ErrBadArgs
		}
	}
	if name == DocsToolName && t.docs != nil {
		return t.docs.Run(args)
	}
	cfg, ok := t.byName[name]
	if !ok {
		return ToolResult{Tool: name, ExitCode: -1, Error: fmt.Sprintf("unknown tool %q; tools: %s", name, strings.Join(t.Names(), ", "))}, ErrUnknownTool
	}
	if len(args) == 0 || !slices.Contains(cfg.Subcommands, args[0]) {
		return ToolResult{Tool: name, ExitCode: -1, Error: fmt.Sprintf("%s needs one of: %s", name, strings.Join(cfg.Subcommands, ", "))}, ErrSubcommandNotAllowed
	}
	for _, a := range args[1:] {
		for _, f := range cfg.ForbiddenFlags {
			if a == f || strings.HasPrefix(a, f+"=") {
				return ToolResult{Tool: name, ExitCode: -1, Error: fmt.Sprintf("flag %s is not allowed through this bot", f)}, ErrForbiddenFlag
			}
		}
	}
	env := append([]string(nil), cfg.Env...)
	for k, path := range cfg.EnvFromFiles {
		raw, err := os.ReadFile(path)
		if err != nil {
			return ToolResult{Tool: name, ExitCode: -1, Error: fmt.Sprintf("read %s: %v", k, err)}, ""
		}
		env = append(env, k+"="+strings.TrimSpace(string(raw)))
	}
	full := append(append([]string(nil), cfg.ArgsPrefix...), args...)
	res := runProcess(ctx, cfg.Command, full, env, time.Duration(cfg.TimeoutSeconds)*time.Second, t.limit)
	out := ToolResult{Tool: name, ExitCode: res.ExitCode, Output: res.Output, Truncated: res.Truncated, Error: res.Error}
	if res.TimedOut {
		return out, ErrTimeout
	}
	return out, ""
}

// hasControl reports control characters other than tab and newline.
func hasControl(s string) bool {
	for _, r := range s {
		if r < 0x20 && r != '\t' && r != '\n' || r == 0x7f {
			return true
		}
	}
	return false
}
