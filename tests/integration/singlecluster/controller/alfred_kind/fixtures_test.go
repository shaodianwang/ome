package alfred_kind

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// alfredFixture is one workload alfred can observe: model + runtime + ISVC.
type alfredFixture struct {
	CBM  *v1beta1.ClusterBaseModel
	CSR  *v1beta1.ClusterServingRuntime
	ISVC *v1beta1.InferenceService
	// raw marks a RawDeployment engine, which has no InferenceReplica.
	raw bool
}

type fixtureOptions struct {
	name           string
	image          string
	gpus           int64
	engineReplicas int
	gangWorkers    int
	withDecoder    bool
	rawEngine      bool
	readinessProbe bool
	isvcAnnots     map[string]string
	migrationMode  v1beta1.MigrationPolicyMode
	env            []corev1.EnvVar
}

type fixtureOption func(*fixtureOptions)

func withName(n string) fixtureOption        { return func(o *fixtureOptions) { o.name = n } }
func withEngineReplicas(n int) fixtureOption { return func(o *fixtureOptions) { o.engineReplicas = n } }
func withGang(workersPerInstance int) fixtureOption {
	return func(o *fixtureOptions) { o.gangWorkers = workersPerInstance }
}
func withDecoder() fixtureOption   { return func(o *fixtureOptions) { o.withDecoder = true } }
func withRawEngine() fixtureOption { return func(o *fixtureOptions) { o.rawEngine = true } }
func withMigrationMode(m v1beta1.MigrationPolicyMode) fixtureOption {
	return func(o *fixtureOptions) { o.migrationMode = m }
}
func withISVCAnnotation(k, v string) fixtureOption {
	return func(o *fixtureOptions) {
		if o.isvcAnnots == nil {
			o.isvcAnnots = map[string]string{}
		}
		o.isvcAnnots[k] = v
	}
}

// withNodeScopedFailure makes every container fail readiness only on failNode
// (fake-serving's FAIL_ON_NODE mode plus the probe that surfaces it), so an
// Instance placed there parks Running-but-unready.
func withNodeScopedFailure(failNode string) fixtureOption {
	return func(o *fixtureOptions) {
		o.env = append(o.env,
			corev1.EnvVar{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}},
			corev1.EnvVar{Name: "FAIL_ON_NODE", Value: failNode},
		)
		o.readinessProbe = true
	}
}

// gpuContainer is the fake-serving container with a GPU limit. Extended
// resources require request == limit, so both are set.
func gpuContainer(image string, gpus int64, env []corev1.EnvVar, probe bool) corev1.Container {
	c := corev1.Container{
		Name:  "ome-container",
		Image: image,
		Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP}},
		Env:   append([]corev1.EnvVar(nil), env...),
	}
	if gpus > 0 {
		q := *resource.NewQuantity(gpus, resource.DecimalSI)
		c.Resources = corev1.ResourceRequirements{
			Limits:   corev1.ResourceList{gpuResource: q},
			Requests: corev1.ResourceList{gpuResource: q},
		}
	}
	if probe {
		c.ReadinessProbe = &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt(8080)}},
			PeriodSeconds:    1,
			FailureThreshold: 2,
		}
	}
	return c
}

// nsSuffix is the random tail util.CreateNamespaceFromPrefix appends to a
// namespace name, which is what makes per-spec cluster-scoped names unique.
func nsSuffix(ns string) string {
	const tail = 5
	if len(ns) <= tail {
		return ns
	}
	return ns[len(ns)-tail:]
}

// newAlfredFixture builds a minimum-viable OMENative ISVC (no lifecycle block
// unless asked; these suites run no defaulting webhook, the workload planner
// fills the rest). Defaults: one single-pod engine Instance requesting one GPU.
func newAlfredFixture(ns string, opts ...fixtureOption) *alfredFixture {
	o := &fixtureOptions{name: "alfred-fixture", image: "fake-serving:v1", gpus: 1, engineReplicas: 1}
	for _, opt := range opts {
		opt(o)
	}
	// Cluster-scoped names carry the namespace's random tail so back-to-back
	// specs never share a model or runtime. The CBM name also becomes a
	// node-label key, so it stays short.
	suffix := nsSuffix(ns)
	cbm := kindfix.StandardClusterBaseModel(o.name+"-"+suffix+"-cbm", kindfix.StandardModelStorageURI)
	csr := kindfix.StandardClusterServingRuntime(o.name + "-" + suffix + "-csr")

	mode := string(constants.OMENative)
	if o.rawEngine {
		mode = string(constants.RawDeployment)
	}
	container := func() corev1.Container { return gpuContainer(o.image, o.gpus, o.env, o.readinessProbe) }
	engine := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: mode}},
		PodSpec:                v1beta1.PodSpec{Containers: []corev1.Container{container()}},
	}
	if o.engineReplicas != 1 {
		mr := o.engineReplicas
		engine.MinReplicas = &mr
		engine.MaxReplicas = o.engineReplicas
	}
	if o.gangWorkers > 0 {
		size := o.gangWorkers
		engine.Leader = &v1beta1.LeaderSpec{PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{container()}}}
		engine.Worker = &v1beta1.WorkerSpec{Size: &size, PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{container()}}}
	}
	if o.migrationMode != "" {
		engine.Lifecycle = &v1beta1.LifecycleSpec{MigrationPolicy: &v1beta1.MigrationPolicy{Mode: o.migrationMode}}
	}

	wrapper := v1beta1testing.MakeInferenceService(o.name, ns).
		Model(cbm.Name).
		Runtime(csr.Name).
		DeploymentMode(constants.OMENative).
		Engine(engine)
	for k, v := range o.isvcAnnots {
		wrapper = wrapper.Annotation(k, v)
	}
	if o.withDecoder {
		wrapper = wrapper.Decoder(&v1beta1.DecoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
			},
			PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{container()}},
		})
	}
	return &alfredFixture{CBM: cbm, CSR: csr, ISVC: wrapper.Obj(), raw: o.rawEngine}
}

// applyFixture creates the CBM, CSR, and ISVC in order, waiting for the
// in-process manager's cache to see the model + runtime first.
func applyFixture(g gomega.Gomega, f *alfredFixture) {
	mgr := kindutil.Suite().Manager
	g.ExpectWithOffset(1, mgr).NotTo(gomega.BeNil(), "controller manager not running (an earlier restart failed)")
	kindfix.ApplyModelRuntime(suiteCtx, g, k8sClient, f.CBM, f.CSR, kindfix.ApplyModelRuntimeOpts{
		CachedReader: mgr.GetClient(),
	})
	g.ExpectWithOffset(1, k8sClient.Create(suiteCtx, f.ISVC)).To(gomega.Succeed(), "create ISVC")
}

func deferFixtureCleanup(f *alfredFixture) {
	ginkgo.DeferCleanup(func() {
		if f != nil {
			kindfix.CleanupModelRuntime(suiteCtx, k8sClient, f.CBM, f.CSR, f.ISVC)
		}
	})
}

// readyOn creates the fixture pinned to node, waits for n Ready engine
// Instances (n Ready pods for a RawDeployment fixture), releases the pin, and
// waits out the placement cooldown. Cleanup is registered before the create so
// a failed create still tears down.
func readyOn(f *alfredFixture, node string, n int) {
	release := pinToNode(node)
	deferFixtureCleanup(f)
	applyFixture(gomega.Default, f)
	if f.raw {
		waitRawPodsReady(f.ISVC.Namespace, f.ISVC.Name, n)
	} else {
		waitInstancesReady(f.ISVC.Namespace, f.ISVC.Name, v1beta1.EngineComponent, n)
	}
	release()
	waitPlacementCooldown()
}

func isvcKey(f *alfredFixture) client.ObjectKey {
	return client.ObjectKey{Namespace: f.ISVC.Namespace, Name: f.ISVC.Name}
}

func workloadID(f *alfredFixture) string { return f.ISVC.Namespace + "/" + f.ISVC.Name }

// annotateISVC merges annotations onto the ISVC as the suite's admin identity.
func annotateISVC(g gomega.Gomega, key client.ObjectKey, annotations map[string]string) {
	g.EventuallyWithOffset(1, func(gg gomega.Gomega) {
		isvc := &v1beta1.InferenceService{}
		gg.Expect(k8sClient.Get(suiteCtx, key, isvc)).To(gomega.Succeed())
		base := isvc.DeepCopy()
		if isvc.Annotations == nil {
			isvc.Annotations = map[string]string{}
		}
		for k, v := range annotations {
			isvc.Annotations[k] = v
		}
		gg.Expect(k8sClient.Patch(suiteCtx, isvc, client.MergeFrom(base))).To(gomega.Succeed())
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// freshIR reads the InferenceReplica that owns one Component of an ISVC.
func freshIR(g gomega.Gomega, ns, isvc string, component v1beta1.ComponentType) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{}
	name := irprojector.InferenceReplicaName(isvc, component)
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns, Name: name}, ir)).To(gomega.Succeed())
	return ir
}

// instanceRows decodes the per-Instance rows regardless of the stored
// representation (DenseV1 or ColumnarV2).
func instanceRows(g gomega.Gomega, ns, isvc string, component v1beta1.ComponentType) []v1beta1.OMENativeInstanceStatus {
	rows, err := util.DecodedInstanceStatuses(freshIR(g, ns, isvc, component))
	g.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "decode instance rows")
	return rows
}

// waitInstancesReady blocks until the Component has exactly n Ready Instances.
func waitInstancesReady(ns, isvc string, component v1beta1.ComponentType, n int) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		rows := instanceRows(g, ns, isvc, component)
		g.Expect(rows).To(gomega.HaveLen(n))
		for _, r := range rows {
			g.Expect(r.Phase).To(gomega.Equal(v1beta1.OMENativeInstanceReady), "instance %d", r.Index)
			g.Expect(r.Operation).To(gomega.BeNil(), "instance %d has an operation in flight", r.Index)
		}
		g.Expect(freshIR(g, ns, isvc, component).Status.ReadyReplicas).To(gomega.Equal(int32(n)))
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// waitRawPodsReady waits for n Ready pods of a RawDeployment ISVC (no IR).
func waitRawPodsReady(ns, isvc string, n int) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		ready := 0
		for _, p := range omenativePods(g, ns, isvc) {
			if isLivePod(&p) && podIsReady(&p) {
				ready++
			}
		}
		g.Expect(ready).To(gomega.Equal(n))
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

func migrationEntries(g gomega.Gomega, ns, isvc string, component v1beta1.ComponentType) []v1beta1.MigrationStatus {
	return freshIR(g, ns, isvc, component).Status.Migrations
}

func findMigration(entries []v1beta1.MigrationStatus, uuid string) *v1beta1.MigrationStatus {
	for i := range entries {
		if entries[i].RequestUUID == uuid {
			return &entries[i]
		}
	}
	return nil
}

// stopOnFailedMigration ends the enclosing wait at once when the controller
// records m Failed: a failed migration is a verdict, not something to wait out.
func stopOnFailedMigration(isvc string, m *v1beta1.MigrationStatus) {
	if m.Phase == v1beta1.MigrationPhaseFailed {
		gomega.StopTrying(fmt.Sprintf("migration %s of %s instance %d failed: %s", m.RequestUUID, isvc, m.SourceInstance, m.Message)).Now()
	}
}

// expectMigrationCompleted asserts, within one poll, that the controller
// records uuid Completed on the Component's InferenceReplica and returns the
// record; a Failed record ends the enclosing wait at once.
func expectMigrationCompleted(g gomega.Gomega, ns, isvc string, component v1beta1.ComponentType, uuid string) v1beta1.MigrationStatus {
	m := findMigration(migrationEntries(g, ns, isvc, component), uuid)
	g.ExpectWithOffset(1, m).NotTo(gomega.BeNil(), "migration %s not recorded on the %s InferenceReplica yet", uuid, component)
	stopOnFailedMigration(isvc, m)
	g.ExpectWithOffset(1, m.Phase).To(gomega.Equal(v1beta1.MigrationPhaseCompleted), "migration %s phase", uuid)
	return *m
}

// waitMigrationCompleted blocks until the controller records uuid Completed
// and returns the record; a Failed record ends the wait at once.
func waitMigrationCompleted(ns, isvc string, component v1beta1.ComponentType, uuid string) v1beta1.MigrationStatus {
	var out v1beta1.MigrationStatus
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		out = expectMigrationCompleted(g, ns, isvc, component, uuid)
	}).WithTimeout(migrationTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
	return out
}

// omenativePods lists the pods of one ISVC (every deployment mode stamps the
// ome.io/inferenceservice label).
func omenativePods(g gomega.Gomega, ns, isvc string) []corev1.Pod {
	pods := &corev1.PodList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, pods, client.InNamespace(ns),
		client.MatchingLabels{constants.InferenceServiceLabel: isvc})).To(gomega.Succeed())
	return pods.Items
}

// isLivePod reports whether a pod still occupies its node: not Terminating
// and not in a terminal phase.
func isLivePod(p *corev1.Pod) bool {
	return p.DeletionTimestamp == nil && p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed
}

// livePodsOnNode keeps the live pods bound to node.
func livePodsOnNode(pods []corev1.Pod, node string) []corev1.Pod {
	var out []corev1.Pod
	for i := range pods {
		if pods[i].Spec.NodeName == node && isLivePod(&pods[i]) {
			out = append(out, pods[i])
		}
	}
	return out
}

// livePodsOfComponent keeps the live pods that carry component's label.
func livePodsOfComponent(pods []corev1.Pod, component v1beta1.ComponentType) []corev1.Pod {
	var out []corev1.Pod
	for i := range pods {
		if isLivePod(&pods[i]) && pods[i].Labels[constants.OMEComponentLabel] == string(component) {
			out = append(out, pods[i])
		}
	}
	return out
}

// livePodsByRunner groups the live pods by runner label, so a gang's leader
// and worker are asserted apart.
func livePodsByRunner(pods []corev1.Pod) map[v1beta1.RunnerName][]corev1.Pod {
	out := map[v1beta1.RunnerName][]corev1.Pod{}
	for i := range pods {
		if !isLivePod(&pods[i]) {
			continue
		}
		runner := v1beta1.RunnerName(pods[i].Labels[query.LabelRunner])
		out[runner] = append(out[runner], pods[i])
	}
	return out
}

// expectGangShape asserts the runner-keyed pods are exactly one leader and one
// worker and returns them in that order.
func expectGangShape(g gomega.Gomega, byRunner map[v1beta1.RunnerName][]corev1.Pod) (leader, worker corev1.Pod) {
	g.ExpectWithOffset(1, byRunner[v1beta1.RunnerNameLeader]).To(gomega.HaveLen(1), "one leader")
	g.ExpectWithOffset(1, byRunner[v1beta1.RunnerNameWorker]).To(gomega.HaveLen(1), "one worker")
	g.ExpectWithOffset(1, byRunner).To(gomega.HaveLen(2), "no pod outside the leader and worker runners")
	return byRunner[v1beta1.RunnerNameLeader][0], byRunner[v1beta1.RunnerNameWorker][0]
}

func podIsReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// waitPodsOffNode blocks until no live pod of the ISVC remains on node.
func waitPodsOffNode(ns, isvc, node string) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		g.Expect(livePodsOnNode(omenativePods(g, ns, isvc), node)).To(gomega.BeEmpty())
	}).WithTimeout(migrationTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// fillerPod is a plain (non-OME) GPU pod pinned to node by nodeName; alfred
// counts it as occupancy but never moves it.
func fillerPod(ns, name, node string, gpus int64) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"alfred-kind/filler": "true"}},
		Spec: corev1.PodSpec{
			NodeName:      node,
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers:    []corev1.Container{gpuContainer("fake-serving:v1", gpus, nil, false)},
		},
	}
}

// createFiller creates a filler pod and waits for it to run. Deleted at spec
// end. A pod the kubelet rejects ends the wait at once with the rejection.
func createFiller(g gomega.Gomega, ns, name, node string, gpus int64) *corev1.Pod {
	pod := fillerPod(ns, name, node, gpus)
	g.ExpectWithOffset(1, k8sClient.Create(suiteCtx, pod)).To(gomega.Succeed(), "create filler %s", name)
	ginkgo.DeferCleanup(func() {
		_ = client.IgnoreNotFound(k8sClient.Delete(suiteCtx, pod))
	})
	gomega.EventuallyWithOffset(1, func(gg gomega.Gomega) {
		fresh := &corev1.Pod{}
		gg.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(pod), fresh)).To(gomega.Succeed())
		if fresh.Status.Phase == corev1.PodFailed {
			gomega.StopTrying(fmt.Sprintf("filler %s failed: %s: %s", name, fresh.Status.Reason, fresh.Status.Message)).Now()
		}
		gg.Expect(fresh.Status.Phase).To(gomega.Equal(corev1.PodRunning))
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
	return pod
}

// waitNodeGPUsFree blocks until no pod bound to node holds a GPU. The kubelet
// admits a nodeName-bound pod only if its GPUs are free at that instant, and a
// previous spec's pods may still be releasing theirs. A Terminating pod holds
// its GPUs until it exits, so only a terminal phase counts as released.
func waitNodeGPUsFree(node string) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		pods := &corev1.PodList{}
		g.Expect(k8sClient.List(suiteCtx, pods, client.MatchingFields{"spec.nodeName": node})).To(gomega.Succeed())
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
				continue
			}
			g.Expect(podGPURequest(p)).To(gomega.BeZero(), "pod %s/%s still holds GPUs on %s", p.Namespace, p.Name, node)
		}
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// podGPURequest sums the GPU request of the pod's containers; an extended
// resource carries equal request and limit, so the limit stands in when only
// it is set.
func podGPURequest(pod *corev1.Pod) int64 {
	var total int64
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if q, ok := c.Resources.Requests[gpuResource]; ok {
			total += q.Value()
		} else if q, ok := c.Resources.Limits[gpuResource]; ok {
			total += q.Value()
		}
	}
	return total
}

// pendingGPUPod creates an unschedulable pod requesting gpus GPUs with no
// node pin; it stays Pending while no node has that much free capacity.
func pendingGPUPod(g gomega.Gomega, ns, name string, gpus int64) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers:    []corev1.Container{gpuContainer("fake-serving:v1", gpus, nil, false)},
		},
	}
	g.ExpectWithOffset(1, k8sClient.Create(suiteCtx, pod)).To(gomega.Succeed())
	ginkgo.DeferCleanup(func() { _ = client.IgnoreNotFound(k8sClient.Delete(suiteCtx, pod)) })
	return pod
}

// nonEmergencyPendingAgeMinutes keeps a spec's pending pod a pressure signal
// only. A pending pod older than emergencyPendingAgeMinutes that a move would
// seat makes the move an emergency, which doubles its score and passes the
// maintenance windows; the suite's base value is one minute.
const nonEmergencyPendingAgeMinutes = 24 * 60

// fragmentedPair lays out fragmentation alfred can fix with one move: one
// single-GPU Instance on each of the first two workers, the third worker
// cordoned, and a pending pod asking for a whole worker, which fits only once
// one of the two is emptied. The pending demand weights the score toward
// whole-worker slots and the emptiable worker is reclaimable capacity, so
// moving either Instance onto the other worker scores as an executable
// defragmentation. The pending pod is created after the cordon so it cannot
// take the third worker instead.
func fragmentedPair(ns string) (left, right *alfredFixture, pending *corev1.Pod) {
	left = newAlfredFixture(ns, withName("frag-left"))
	right = newAlfredFixture(ns, withName("frag-right"))
	placements := []struct {
		fix  *alfredFixture
		node string
	}{{left, workers[0]}, {right, workers[1]}}
	for _, p := range placements {
		release := pinToNode(p.node)
		deferFixtureCleanup(p.fix)
		applyFixture(gomega.Default, p.fix)
		waitInstancesReady(ns, p.fix.ISVC.Name, v1beta1.EngineComponent, 1)
		release()
	}
	cordon(workers[2])
	pending = pendingGPUPod(gomega.Default, ns, "frag-pending", gpusPerWorker)
	waitUnschedulable(pending)
	return left, right, pending
}

// waitUnschedulable blocks until the scheduler has recorded that pod fits
// nowhere; alfred counts the pod's pending age from that condition.
func waitUnschedulable(pod *corev1.Pod) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		fresh := freshPod(g, pod)
		g.Expect(fresh.Spec.NodeName).To(gomega.BeEmpty(), "pod %s was scheduled", pod.Name)
		g.Expect(fresh.Status.Conditions).To(gomega.ContainElement(gomega.SatisfyAll(
			gomega.HaveField("Type", corev1.PodScheduled),
			gomega.HaveField("Status", corev1.ConditionFalse),
		)), "pod %s has no unschedulable condition yet", pod.Name)
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

func freshPod(g gomega.Gomega, pod *corev1.Pod) *corev1.Pod {
	fresh := &corev1.Pod{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(pod), fresh)).To(gomega.Succeed())
	return fresh
}

// singleLivePod returns the one live pod of a single-Instance fixture on node.
func singleLivePod(g gomega.Gomega, f *alfredFixture, node string) corev1.Pod {
	live := livePodsOnNode(omenativePods(g, f.ISVC.Namespace, f.ISVC.Name), node)
	g.ExpectWithOffset(1, live).To(gomega.HaveLen(1), "%s pods on %s", workloadID(f), node)
	return live[0]
}
