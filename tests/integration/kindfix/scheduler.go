package kindfix

import (
	"os"
	"strconv"
	"strings"

	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// EnvScheduler names the scheduler every KIND suite stamps on its fixture
// runtimes and installs into its cluster; empty means the cluster default.
// The pipeline sets it to ome-scheduler for the scheduler dimension of every
// KIND lane; locally: OME_TEST_SCHEDULER=ome-scheduler make test-kind.
const EnvScheduler = "OME_TEST_SCHEDULER"

// EnvSchedulerImage overrides the local image tag the suite loads for the
// scheduler; the CI runner points it at the baked archive it tagged.
const EnvSchedulerImage = "OME_TEST_SCHEDULER_IMAGE"

// EnvSchedulerWorkers overrides the worker count a scheduler-mode suite insists
// on (default 6, one gang member per node).
const EnvSchedulerWorkers = "OME_TEST_SCHEDULER_WORKERS"

const defaultSchedulerImage = "ome-scheduler:kind-local"

// SchedulerName is the scheduler the fixtures select, or "" for the default.
func SchedulerName() string {
	return strings.TrimSpace(os.Getenv(EnvScheduler))
}

// SchedulerFromEnv is the SuiteOpts.Scheduler value for the environment: nil
// when no scheduler is requested.
func SchedulerFromEnv() *kindutil.SchedulerInstall {
	name := SchedulerName()
	if name == "" {
		return nil
	}
	image := strings.TrimSpace(os.Getenv(EnvSchedulerImage))
	if image == "" {
		image = defaultSchedulerImage
	}
	install := &kindutil.SchedulerInstall{Name: name, Image: image}
	if raw := strings.TrimSpace(os.Getenv(EnvSchedulerWorkers)); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			install.MinWorkers = n
		}
	}
	return install
}
