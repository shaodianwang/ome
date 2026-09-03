package kindutil

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// DeployedOME switches a suite from an in-process manager to an OME install
// in the KIND cluster managed by RunKindSuite.
//
// This mode supports admission and chart-wiring contracts that an in-process
// manager cannot exercise.
type DeployedOME struct {
	// Script is a repo-relative path to an executable that installs OME into
	// the suite's KIND cluster. Empty means the suite assumes OME is already
	// installed in that cluster.
	Script string

	// Args are passed to Script verbatim, after the cluster flag.
	Args []string

	// Env entries ("K=V") are added to Script's environment.
	Env []string

	// Timeout bounds the install command. Values less than or equal to zero
	// add no deadline; cancellation and deadlines from the caller still apply.
	Timeout time.Duration
}

// deployOME runs opts.Script against the suite's KIND cluster. A nil or
// Script-less DeployedOME is a no-op for a cluster with OME already installed.
//
// Output is streamed so progress remains visible during long-running installs.
func deployOME(ctx context.Context, clusterName string, d *DeployedOME) error {
	if d == nil || d.Script == "" {
		return nil
	}
	script, err := resolveContextDir(d.Script)
	if err != nil {
		return fmt.Errorf("deployOME: resolve %s: %w", d.Script, err)
	}
	if _, err := os.Stat(script); err != nil {
		return fmt.Errorf("deployOME: %w", err)
	}
	root, err := RepoRoot()
	if err != nil {
		return fmt.Errorf("deployOME: repo root: %w", err)
	}

	runCtx := ctx
	cancel := func() {}
	if d.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, d.Timeout)
	}
	defer cancel()

	args := append([]string{"--cluster", clusterName}, d.Args...)
	cmd := exec.CommandContext(runCtx, script, args...)
	// Keep the installer and every command it launches in one private process
	// group so cancellation cannot leave cluster-mutating children behind.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			if errors.Is(err, syscall.ESRCH) {
				return os.ErrProcessDone
			}
			return err
		}
		return nil
	}
	cmd.Dir = root
	cmd.Env = append(os.Environ(), d.Env...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Printf("--- deploying OME: %s %s ---\n", d.Script, strings.Join(args, " "))
	if err := cmd.Run(); err != nil {
		if runCtx.Err() != nil {
			return fmt.Errorf("deployOME: %s %s: %w", d.Script, strings.Join(args, " "), runCtx.Err())
		}
		return fmt.Errorf("deployOME: %s %s: %w", d.Script, strings.Join(args, " "), err)
	}
	return nil
}
