// Command kubectl-qt renders the AcceleratorQuota hierarchy as a tree.
//
// kubectl dispatches `kubectl qt` to this binary when it is on PATH, so the
// plugin is installed by dropping it there or through krew. See
// docs/kubectl-qt/README.md.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	// Registers the oidc, gcp, and azure auth providers. A plugin is run
	// against whatever kubeconfig the operator already has, and without this a
	// context using one of them fails to authenticate at all.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/ome-projects/ome/pkg/kubectl/quotatree"
)

func main() {
	// A read of the whole quota tree can block on an unreachable apiserver, so
	// an interrupt has to reach the in-flight request rather than only the
	// shell that started it.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := quotatree.NewCommand(os.Stdout, os.Stderr)
	if err := cmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
