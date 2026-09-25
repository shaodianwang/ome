package kindfix

import (
	"os"
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
	return &kindutil.SchedulerInstall{Name: name, Image: image}
}
