package alfred_kind

import (
	"time"

	"github.com/ome-projects/ome/tests/integration/kindfix"
)

const (
	kindShortTimeout  = kindfix.ShortTimeout
	kindMediumTimeout = kindfix.MediumTimeout
	kindLongTimeout   = kindfix.LongTimeout
	kindPollInterval  = kindfix.PollInterval

	// Loop and dispatch options alfred is deployed with (alfredValues): the
	// leader runs one decision pass per decisionInterval; an entry with no
	// status row acknowledgementTimeout after it was created is stalled, and
	// the transition into stalled or failed arms a dispatch backoff of
	// failureBackoff.
	decisionInterval       = 5 * time.Second
	acknowledgementTimeout = 20 * time.Second
	failureBackoff         = 10 * time.Second
	// dispatchTimeout covers alfred's recentPlacementCooldown (one minute after
	// a pod is placed), a decision tick, and one real migration on a real
	// kubelet (surge, readiness, drain).
	dispatchTimeout = 4 * time.Minute
	// quietPeriod is how long a spec watches for something that must NOT
	// happen: the placement cooldown plus several decision ticks.
	quietPeriod = placementCooldown + 5*decisionInterval
	// quietPollInterval paces the checks inside a quiet period; a few per
	// decision tick is enough, and each poll reads several objects.
	quietPollInterval = 2 * time.Second
	// stallQuietWindow is how long a stalled request is watched for a re-issue
	// or a withdrawal: nine decision ticks. The dispatcher withholds on the
	// unresolved entry alone, so the placement-cooldown share of quietPeriod
	// does not apply.
	stallQuietWindow = 9 * decisionInterval
	// dispatchQuietWindow is the window over which a dispatcher that withholds
	// every pass for a reason independent of cooldowns is watched: nine
	// decision ticks.
	dispatchQuietWindow = 9 * decisionInterval
	// quietWindowMinPasses is the minimum number of decision passes a leader
	// must complete across a nine-tick quiet window to count as live on the
	// configured interval; a timer that fires only twice cannot pass.
	quietWindowMinPasses = 5
	// migrationTimeout bounds one controller-side migration once accepted.
	migrationTimeout = 3 * time.Minute
	// deployedPlacementCooldown is the recentPlacementCooldown alfred is
	// deployed with: an Instance whose pod was placed less than this long ago
	// is held back. placementCooldown is waited once after a fixture is Ready,
	// with a decision tick on top, so that hold never explains a withheld
	// dispatch.
	deployedPlacementCooldown = time.Duration(recentPlacementCooldownMinutes) * time.Minute
	placementCooldown         = deployedPlacementCooldown + decisionInterval
	// deployedHealthFloor is the healthCooldownFloorMinutes alfred is deployed
	// with: the least gap between one health evacuation's recorded completion
	// and the next request for the same workload. The arbiter measures it
	// from the InferenceReplica's CompletedAt, the dispatcher from the
	// journal's copy of the same instant, and the new entry's CreatedAt is
	// taken after both gates. healthCooldownFloor adds the decision tick that
	// observes the floor's end; a spec that evacuates n Instances of one
	// workload budgets n-1 of these.
	deployedHealthFloor = time.Duration(healthCooldownFloorMinutes) * time.Minute
	healthCooldownFloor = deployedHealthFloor + decisionInterval
	// eventSkew widens the spec-start watermark on event lookups: metav1.Time
	// is second-granular and the KIND node clock may drift a little from the
	// host, so the watermark is applied as specStart minus this.
	eventSkew = 5 * time.Second
	// cadenceWindow is how long BeforeSuite watches the leader's decision pass
	// count: four configured intervals, so two completed passes cannot be one
	// early pass alone even when each pass takes most of an interval (the
	// timer re-arms after a pass completes). cadencePoll paces the reads.
	cadenceWindow = 4 * decisionInterval
	cadencePoll   = time.Second
)

// serialEvacuationTimeout bounds n evacuations off one node: alfred holds one
// request in flight at a time, and between two evacuations of the same
// workload it waits out the health cooldown floor after the earlier one
// completes, so n-1 floors is the worst case (every Instance in one workload).
func serialEvacuationTimeout(n int) time.Duration {
	return dispatchTimeout + time.Duration(n)*migrationTimeout + time.Duration(n-1)*healthCooldownFloor
}

// waitPlacementCooldown sleeps out placementCooldown, returning early when the
// suite context ends.
func waitPlacementCooldown() {
	select {
	case <-time.After(placementCooldown):
	case <-suiteCtx.Done():
	}
}
