// Command ome-scheduler is the OME accelerator scheduler: the upstream
// kube-scheduler, built as a library, with the OME placement plugin registered
// alongside the built-in plugins. See OEP-0022.
package main

import (
	"os"

	"k8s.io/component-base/cli"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	"github.com/ome-projects/ome/scheduler/pkg/plugins/gangpack"
)

func main() {
	// NewSchedulerCommand returns the real kube-scheduler cobra command with our
	// plugin added to the registry; the scheduler config (ConfigMap) decides at
	// which hooks it runs. Nothing else about upstream is forked.
	cmd := app.NewSchedulerCommand(
		app.WithPlugin(gangpack.Name, gangpack.New),
	)
	os.Exit(cli.Run(cmd))
}
