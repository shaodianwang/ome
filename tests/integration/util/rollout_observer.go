package util

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/ome-projects/ome/pkg/constants"
)

const (
	// RolloutRevisionHashLabel is the OMENative per-revision pod label
	// (query.LabelRevisionHash). A surge/recreate update stamps a NEW value;
	// an in-place update keeps it.
	RolloutRevisionHashLabel = "ome.io/revision-hash"
	// RolloutTriggerAnnotation is the per-Component annotation the runner bumps
	// on EVERY coordination-group member to trigger a roll. Component
	// annotations fold into the revision hash, so bumping it rolls that
	// Component (surge/recreate) or patches the live pod (in-place). Kept as a
	// non-ome.io key (matching the playground's proven trigger) so it isn't
	// filtered out of the revision-hash computation as a system annotation.
	RolloutTriggerAnnotation = "perm.test/rev"
	RolloutTriggerValue      = "v2"
	// RolloutPairingProtocolLabel is the P/D pairing cohort label
	// (query.LabelPairingProtocol): stamped on pods at render and restamped
	// in place on an in-place update; carried on per-revision routing
	// Services in METADATA only, never the selector. Absent = pairs with
	// anything.
	RolloutPairingProtocolLabel = "ome.io/pairing-protocol"
	// PodServingConditionType is the controller-managed pod readiness gate
	// (query.ServingConditionType) marking a pod as in the serving rotation.
	PodServingConditionType = "ome.io/serving"
)

// PairAliveSustainWindow is how long an unpaired engine/decoder state must
// persist (seconds) before it counts as a pair-alive violation. A REAL pair
// break lasts at least one pod-startup cycle (the new cohort must come Ready
// before a pair can re-form); a sub-window blip is a watch-interleave
// transient between a label restamp and a serving flip. Callers assert
// PairAliveWorst() < this.
const PairAliveSustainWindow = 5.0

// ratioSustainWindow is how long a serving-ratio excursion must persist before
// RatioWorst counts it as a band violation. Shorter excursions are
// cross-dispatcher surge/drain interleave transients that the eventually-
// consistent RatioBalanced gate corrects on the next reconcile (each Component
// is gated per-action, so a peer's status can lag for a reconcile or two). The
// playground's 1Hz sampling missed these; a watch sees every event, so we
// debounce explicitly and assert on the SUSTAINED ratio the gate converges to.
const ratioSustainWindow = 5.0

// RolloutObserver watches the OMENative pods of one InferenceService and
// derives rollout dynamics from the WATCH STREAM rather than by polling. It is
// the Go counterpart of the playground's run.py watcher: a 100ms poller can
// miss a surge/drain transient that opens and closes between samples, whereas a
// watch sees every pod add/modify/delete.
//
// It maintains a per-pod state map and exposes, per Component:
//   - alive  : pods that exist and are not terminating
//   - ready  : alive pods whose ome-container reports Ready
//   - updated: ready pods that have moved to the new generation, per the
//     Component's update mode —
//     surge   → pod carries a revision-hash NOT present pre-trigger
//     inplace → pod carries the new trigger annotation (same pod)
//   - peak alive (high-water mark, for surge-ceiling checks)
//
// Plus per-Component first-change / fully-rotated timing (for Sequential
// ordering + soak) and a worst RatioBalanced serving-ratio deviation.
//
// Lifecycle: NewRolloutObserver → Start(ctx) → (wait initial Ready via Counts)
// → MarkBaseline(...) at the trigger → read Counts/Peak/Timing → ctx cancel.
type RolloutObserver struct {
	cs   kubernetes.Interface
	ns   string
	isvc string

	mu   sync.Mutex
	pods map[string]*obsPod // pod name → state (nil-safe: deleted pods removed)
	peak map[string]int     // component → high-water alive count

	// Baseline + per-Component config captured at MarkBaseline (the trigger).
	baseline   bool
	baselineAt time.Time
	oldHashes  map[string]bool            // revision hashes present pre-trigger
	oldUIDs    map[string]map[string]bool // component → pod UIDs pre-trigger
	modes      map[string]string          // component → "surge" | "inplace"
	want       map[string]int             // component → desired replica count N
	bumped     map[string]bool            // components perturbed by the trigger
	startedTS  map[string]float64         // component → secs-post-trigger first change
	doneTS     map[string]float64         // component → secs-post-trigger fully rotated
	minReady   map[string]int             // component → lowest ready count seen during the roll

	// RatioBalanced tracking (optional; SetRatioPair enables it). Samples the
	// live serving ratio on every pod event; the band check (RatioWorst) reads
	// the worst SUSTAINED deviation (held >= ratioSustainWindow) so a brief
	// surge/drain interleave isn't mistaken for a violation. ratioInst* retain
	// the instantaneous worst for the (informational) verdict line.
	ratioA, ratioB string
	ratioTarget    float64
	ratioSamples   []ratioSample
	ratioInstDev   float64
	ratioInstAB    [2]int

	// Pair-alive tracking (optional; EnablePairAlive arms it for a
	// non-empty→non-empty pairing-protocol transition). Whenever both engine
	// and decoder have >=1 alive+ready+serving pod, some engine/decoder pod
	// pair must be pairable — equal protocols, or either empty. The gate's
	// two liveness escapes are exempt: once no pair exists (a side at zero or
	// externally destroyed) the tracker DISARMS until a pairable sample
	// reappears, and the 1×1 mutual wall (both sides <=1 serving) is the
	// bounded first-mover window the gate deliberately allows.
	pairTrack       bool
	pairArmed       bool
	pairBadSince    float64 // seconds post-baseline the open unpaired span began; <0 = none
	pairOpenDetail  string  // the open span's violating snapshot
	pairWorst       float64 // longest armed unpaired span observed (seconds)
	pairWorstDetail string
}

type obsPod struct {
	comp    string
	alive   bool
	ready   bool
	serving bool
	hash    string
	pairing string
	uid     string
	newAnno bool
}

// ratioSample is one serving-ratio observation at t seconds post-trigger.
type ratioSample struct {
	t    float64
	dev  float64
	a, b int
}

// NewRolloutObserver builds an observer for ns/isvc from a rest config.
func NewRolloutObserver(cfg *rest.Config, namespace, isvcName string) (*RolloutObserver, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build clientset: %w", err)
	}
	return &RolloutObserver{
		cs:        cs,
		ns:        namespace,
		isvc:      isvcName,
		pods:      map[string]*obsPod{},
		peak:      map[string]int{},
		oldHashes: map[string]bool{},
		oldUIDs:   map[string]map[string]bool{},
		modes:     map[string]string{},
		want:      map[string]int{},
		bumped:    map[string]bool{},
		startedTS: map[string]float64{},
		doneTS:    map[string]float64{},
		minReady:  map[string]int{},
	}, nil
}

// Start launches the watch goroutine. It reconnects when a watch expires
// (channel close) until ctx is done. Returns immediately.
func (o *RolloutObserver) Start(ctx context.Context) {
	selector := fmt.Sprintf("%s=%s,%s=%s",
		LabelManagedByKey, ManagedByOMENativeValue,
		constants.InferenceServicePodLabelKey, o.isvc)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			w, err := o.cs.CoreV1().Pods(o.ns).Watch(ctx, metav1.ListOptions{LabelSelector: selector})
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
					continue
				}
			}
			for ev := range w.ResultChan() {
				pod, ok := ev.Object.(*corev1.Pod)
				if !ok || pod == nil {
					continue
				}
				o.handle(string(ev.Type), pod)
			}
			// ResultChan closed → watch expired; loop reconnects (unless ctx done).
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
}

func podReady(pod *corev1.Pod) bool {
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.Name == constants.MainContainerName {
			return cs.Ready
		}
	}
	return false
}

// podServing reports the controller-managed serving readiness-gate condition.
func podServing(pod *corev1.Pod) bool {
	for i := range pod.Status.Conditions {
		c := &pod.Status.Conditions[i]
		if string(c.Type) == PodServingConditionType {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (o *RolloutObserver) handle(evType string, pod *corev1.Pod) {
	o.mu.Lock()
	defer o.mu.Unlock()

	name := pod.Name
	if evType == "DELETED" {
		delete(o.pods, name)
	} else {
		o.pods[name] = &obsPod{
			comp:    pod.Labels[constants.OMEComponentLabel],
			alive:   pod.DeletionTimestamp == nil,
			ready:   podReady(pod),
			serving: podServing(pod),
			hash:    pod.Labels[RolloutRevisionHashLabel],
			pairing: pod.Labels[RolloutPairingProtocolLabel],
			uid:     string(pod.UID),
			newAnno: pod.Annotations[RolloutTriggerAnnotation] == RolloutTriggerValue,
		}
	}

	agg := o.aggregateLocked()
	for c, v := range agg {
		if v[0] > o.peak[c] {
			o.peak[c] = v[0]
		}
	}

	if !o.baseline {
		return
	}
	now := time.Since(o.baselineAt).Seconds()
	for c := range o.bumped {
		a, rd, up := agg[c][0], agg[c][1], agg[c][2]
		if _, seen := o.startedTS[c]; !seen && (a != o.want[c] || up > 0) {
			o.startedTS[c] = now
		}
		if _, seen := o.doneTS[c]; !seen && a == o.want[c] && rd == o.want[c] && up == o.want[c] {
			o.doneTS[c] = now
		}
	}
	// Track the lowest ready (serving) count each configured Component reaches
	// during the roll, so a spec can assert the serving floor held.
	for c := range o.want {
		if cur, seen := o.minReady[c]; !seen || agg[c][1] < cur {
			o.minReady[c] = agg[c][1]
		}
	}
	if o.ratioA != "" {
		ra, rb := agg[o.ratioA][1], agg[o.ratioB][1]
		if ra > 0 || rb > 0 {
			dev := 9.99
			if rb != 0 {
				dev = abs((float64(ra)/float64(rb))/o.ratioTarget - 1.0)
			}
			o.ratioSamples = append(o.ratioSamples, ratioSample{t: now, dev: dev, a: ra, b: rb})
			if dev > o.ratioInstDev {
				o.ratioInstDev = dev
				o.ratioInstAB = [2]int{ra, rb}
			}
		}
	}
	if o.pairTrack {
		o.samplePairAliveLocked(now)
	}
}

// samplePairAliveLocked folds one pair-alive observation at t seconds
// post-baseline. A violation span opens on an armed unpaired sample and its
// detail records that violating snapshot; the span keeps growing until a
// sample that is pairable, walled, or one-sided arrives. Caller holds mu.
func (o *RolloutObserver) samplePairAliveLocked(now float64) {
	var eng, dec []string // serving pods' pairing protocols per Component
	for _, p := range o.pods {
		if p == nil || !p.alive || !p.ready || !p.serving {
			continue
		}
		switch p.comp {
		case "engine":
			eng = append(eng, p.pairing)
		case "decoder":
			dec = append(dec, p.pairing)
		}
	}
	closeSpan := func() {
		if o.pairBadSince >= 0 {
			if span := now - o.pairBadSince; span > o.pairWorst {
				o.pairWorst = span
				o.pairWorstDetail = o.pairOpenDetail
			}
			o.pairBadSince = -1
		}
	}
	if len(eng) == 0 || len(dec) == 0 {
		// A side has nothing serving — no pair left to protect: disarm until
		// a pairable sample reappears (the gate's no-pair escape).
		o.pairArmed = false
		closeSpan()
		return
	}
	pairable := false
	for _, pe := range eng {
		for _, pd := range dec {
			if pe == "" || pd == "" || pe == pd {
				pairable = true
			}
		}
	}
	if pairable {
		o.pairArmed = true
		closeSpan()
		return
	}
	if len(eng) <= 1 && len(dec) <= 1 {
		// 1×1 mutual wall: the bounded first-mover window the gate allows.
		closeSpan()
		return
	}
	if !o.pairArmed {
		return
	}
	if o.pairBadSince < 0 {
		o.pairBadSince = now
		o.pairOpenDetail = fmt.Sprintf("engine=%v decoder=%v", eng, dec)
	}
	if span := now - o.pairBadSince; span > o.pairWorst {
		o.pairWorst = span
		o.pairWorstDetail = o.pairOpenDetail
	}
}

// EnablePairAlive arms pair-alive tracking. Call it for a non-empty→
// non-empty pairing-protocol transition, together with MarkBaseline.
func (o *RolloutObserver) EnablePairAlive() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pairTrack = true
	o.pairArmed = false
	o.pairBadSince = -1
}

// PairAliveWorst returns the longest ARMED unpaired engine/decoder span
// observed (seconds) and a snapshot of the serving protocols at its worst.
// Spans shorter than PairAliveSustainWindow are watch-interleave transients;
// the caller asserts worst < PairAliveSustainWindow.
func (o *RolloutObserver) PairAliveWorst() (float64, string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.pairWorst, o.pairWorstDetail
}

// aggregateLocked returns component → [alive, ready, updated]. Caller holds mu.
func (o *RolloutObserver) aggregateLocked() map[string][3]int {
	agg := map[string][3]int{}
	for _, p := range o.pods {
		if p == nil || !p.alive {
			continue
		}
		v := agg[p.comp]
		v[0]++
		if p.ready {
			v[1]++
			updated := false
			if o.baseline {
				if o.modes[p.comp] == "inplace" {
					updated = p.newAnno
				} else {
					updated = p.hash != "" && !o.oldHashes[p.hash]
				}
			}
			if updated {
				v[2]++
			}
		}
		agg[p.comp] = v
	}
	return agg
}

// MarkBaseline snapshots the pre-trigger revision hashes + per-Component pod
// UIDs and starts the post-trigger clock. Call it immediately before bumping
// the trigger. want = desired replicas per Component; bumped = the Components
// the trigger perturbs; modes = "surge"|"inplace" per Component.
func (o *RolloutObserver) MarkBaseline(want map[string]int, bumped map[string]bool, modes map[string]string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.oldHashes = map[string]bool{}
	o.oldUIDs = map[string]map[string]bool{}
	for _, p := range o.pods {
		if p == nil || !p.alive {
			continue
		}
		if p.hash != "" {
			o.oldHashes[p.hash] = true
		}
		if o.oldUIDs[p.comp] == nil {
			o.oldUIDs[p.comp] = map[string]bool{}
		}
		o.oldUIDs[p.comp][p.uid] = true
	}
	o.want = want
	o.bumped = bumped
	o.modes = modes
	o.minReady = map[string]int{}
	o.baselineAt = time.Now()
	o.baseline = true
}

// MinReady returns the lowest ready (serving) count a Component reached during
// the roll — the observed serving floor. Returns 0 if the Component was never
// seen post-baseline.
func (o *RolloutObserver) MinReady(comp string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	if v, ok := o.minReady[comp]; ok {
		return v
	}
	return 0
}

// SetRatioPair enables RatioBalanced serving-ratio tracking between two
// Components. target is N(a)/N(b).
func (o *RolloutObserver) SetRatioPair(a, b string, target float64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ratioA, o.ratioB, o.ratioTarget = a, b, target
}

// Counts returns the current per-Component (alive, ready, updated) snapshot.
func (o *RolloutObserver) Counts() map[string][3]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.aggregateLocked()
}

// Peak returns the high-water alive count for a Component.
func (o *RolloutObserver) Peak(comp string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.peak[comp]
}

// CurrentUIDs returns the live pod UIDs for a Component (for in-place UID
// stability checks).
func (o *RolloutObserver) CurrentUIDs(comp string) map[string]bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := map[string]bool{}
	for _, p := range o.pods {
		if p != nil && p.alive && p.comp == comp && p.uid != "" {
			out[p.uid] = true
		}
	}
	return out
}

// BaselineUIDs returns the pre-trigger pod UIDs for a Component.
func (o *RolloutObserver) BaselineUIDs(comp string) map[string]bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := map[string]bool{}
	for u := range o.oldUIDs[comp] {
		out[u] = true
	}
	return out
}

// Timing returns per-Component first-change / fully-rotated times in seconds
// post-trigger (only populated for bumped Components).
func (o *RolloutObserver) Timing() (started, done map[string]float64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	started, done = map[string]float64{}, map[string]float64{}
	for c, t := range o.startedTS {
		started[c] = t
	}
	for c, t := range o.doneTS {
		done[c] = t
	}
	return started, done
}

// RatioWorst returns the worst SUSTAINED serving-ratio deviation (held for at
// least ratioSustainWindow) and the (ready_a, ready_b) at that point. Brief
// surge/drain interleave spikes are debounced out so the assertion reflects the
// ratio the eventually-consistent gate actually converges to, not a sub-second
// cross-dispatcher transient.
func (o *RolloutObserver) RatioWorst() (dev float64, readyA, readyB int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	s := o.ratioSamples
	for i := range s {
		// Each sample's state persists until the next event. Walk the maximal
		// forward run whose dev stays at or above s[i].dev; if that run spans
		// >= ratioSustainWindow, s[i].dev was sustained.
		end := s[i].t
		for j := i; j < len(s); j++ {
			if s[j].dev < s[i].dev {
				break
			}
			end = s[j].t
		}
		if end-s[i].t >= ratioSustainWindow && s[i].dev > dev {
			dev, readyA, readyB = s[i].dev, s[i].a, s[i].b
		}
	}
	return dev, readyA, readyB
}

// RatioInstant returns the worst INSTANTANEOUS serving-ratio deviation (any
// single event) — informational, for the verdict line.
func (o *RolloutObserver) RatioInstant() (dev float64, readyA, readyB int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.ratioInstDev, o.ratioInstAB[0], o.ratioInstAB[1]
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
