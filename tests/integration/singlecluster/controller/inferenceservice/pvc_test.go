package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// End-to-end ISVC + PVC integration. The "PVC support" feature is
// only useful when the ISVC reconciler actually mounts the referenced
// PVC into the runner pod. These specs assert:
//
//   - The generated Deployment's pod template carries a Volume
//     pointing at the PVC named in the BaseModel's storage URI
//   - At least one container has a VolumeMount for that volume at
//     constants.ModelDefaultMountPath with the right SubPath (the
//     URI's path portion)
//   - At least one container has the MODEL_PATH env var pointing at
//     the mount path
//   - The pod template has NO node selector for the model-ready
//     label (PVCs aren't bound to specific nodes; the model-agent
//     doesn't label nodes for PVC-backed models)
//
// All gated by the PVC branches in pkg/render: `parsePVCComponents(p)` in
// UpdateVolumeMounts / UpdatePodSpecVolumes and `isPVCBaseModel(p)` in
// UpdateEnvVariables (through modelPathEnvValue) / UpdatePodSpecNodeSelector.
var _ = ginkgo.Describe("InferenceService + PVC end-to-end", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-pvc-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("PVC-backed ClusterBaseModel → Deployment mounts PVC + sets MODEL_PATH + no model-ready node selector", func() {
		ginkgo.By("creating a PVC in the ISVC's namespace (cluster-scoped BM uses pvc://<ns>:<name>/<path>)")
		pvcName := "isvc-pvc"
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: ns.Name},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		}
		Expect(k8sClient.Create(suiteCtx, pvc)).To(Succeed())
		// Bind the PVC manually — envtest has no volume controller.
		pvc.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(suiteCtx, pvc)).To(Succeed())

		ginkgo.By("creating a PVC-backed ClusterBaseModel pointing at the PVC")
		subPath := "models/llama-7b"
		uri := "pvc://" + ns.Name + ":" + pvcName + "/" + subPath
		cbm := v1beta1testing.MakeClusterBaseModel("isvc-pvc-llama").
			StorageURI(uri).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating a runtime with engineConfig.runner — the canonical runtime pattern; the ISVC's Engine is just a stub to opt in")
		csr := v1beta1testing.MakeClusterServingRuntime("isvc-pvc-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  "ome-container",
				Image: "ghcr.io/test/runtime:test",
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating the ISVC with an empty Engine stub (MergeEngineSpec needs ISVC.Engine != nil; container config comes from the runtime)")
		isvc := v1beta1testing.MakeInferenceService("isvc-pvc-svc", ns.Name).
			Model("isvc-pvc-llama").
			Runtime("isvc-pvc-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("verifying the CSR is readable via the test client (sanity check apiserver state)")
		gotCSR := &v1beta1.ClusterServingRuntime{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: "isvc-pvc-runtime"}, gotCSR)).To(Succeed())
		Expect(gotCSR.Spec.SupportedModelFormats).NotTo(BeEmpty())

		ginkgo.By("waiting for the Deployment and verifying its pod template wires the PVC")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(1))
			pod := deps.Items[0].Spec.Template.Spec

			ginkgo.By("Volume points at the PVC")
			var found *corev1.Volume
			for i := range pod.Volumes {
				if pod.Volumes[i].PersistentVolumeClaim != nil &&
					pod.Volumes[i].PersistentVolumeClaim.ClaimName == pvcName {
					found = &pod.Volumes[i]
					break
				}
			}
			g.Expect(found).NotTo(BeNil(), "pod template must include a PVC volume for ClaimName=%q", pvcName)
			g.Expect(found.PersistentVolumeClaim.ReadOnly).To(BeTrue(),
				"model PVC must be mounted ReadOnly")

			ginkgo.By("at least one container OR init container has a VolumeMount for that volume at the model mount path with the right SubPath")
			allContainers := append([]corev1.Container{}, pod.Containers...)
			allContainers = append(allContainers, pod.InitContainers...)
			var mountFound bool
			for _, c := range allContainers {
				for _, vm := range c.VolumeMounts {
					if vm.Name == found.Name &&
						vm.MountPath == constants.ModelDefaultMountPath &&
						vm.SubPath == subPath {
						mountFound = true
					}
				}
			}
			g.Expect(mountFound).To(BeTrue(),
				"at least one container must mount the PVC volume %q at %q with SubPath=%q (got containers=%+v, initContainers=%+v)",
				found.Name, constants.ModelDefaultMountPath, subPath,
				containerMountsForDebug(pod.Containers), containerMountsForDebug(pod.InitContainers))

			ginkgo.By("at least one container OR init container has MODEL_PATH set to the model mount path")
			var envFound bool
			for _, c := range allContainers {
				for _, e := range c.Env {
					if e.Name == constants.ModelPathEnvVarKey && e.Value == constants.ModelDefaultMountPath {
						envFound = true
					}
				}
			}
			g.Expect(envFound).To(BeTrue(),
				"at least one container must set %s=%s", constants.ModelPathEnvVarKey, constants.ModelDefaultMountPath)

			ginkgo.By("pod template has NO node selector for the model-ready label (PVCs aren't node-bound)")
			// The model-ready selector key has the form
			// "<base-domain>/<modelname>/<modelnamespace>"; for cluster-
			// scoped models, the namespace component is empty. We just
			// assert no key STARTS WITH the OME label prefix that the
			// model-agent uses for per-node ready labels.
			for k := range pod.NodeSelector {
				g.Expect(k).NotTo(HavePrefix("models.ome/"),
					"PVC-backed ISVC pods must not carry a per-node model-ready selector (got %q)", k)
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// Variant: namespaced BaseModel referencing a PVC with the SHORT URI
// form `pvc://<name>/<path>` (no namespace prefix). storage.ParsePVC
// StorageURI accepts both `pvc://<ns>:<name>/<path>` and `pvc://<name>
// /<path>`; the short form means "this same namespace". The mount
// behaviour should be identical to the long form because pods can only
// mount PVCs in their own namespace anyway.
var _ = ginkgo.Describe("InferenceService + PVC (short URI form)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-pvcshort-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("namespaced BaseModel + pvc://name/path → Deployment mounts PVC from the ISVC's namespace", func() {
		pvcName := "isvc-pvc-short"
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: ns.Name},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		}
		Expect(k8sClient.Create(suiteCtx, pvc)).To(Succeed())
		pvc.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(suiteCtx, pvc)).To(Succeed())

		ginkgo.By("creating a NAMESPACED BaseModel using the short URI form pvc://name/path (no ns prefix)")
		subPath := "models/llama-7b"
		uri := "pvc://" + pvcName + "/" + subPath
		bm := v1beta1testing.MakeBaseModel("isvc-pvc-short-llama", ns.Name).
			StorageURI(uri).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		csr := v1beta1testing.MakeClusterServingRuntime("isvc-pvc-short-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating ISVC with NamespacedModel (BaseModel kind) referencing the short-URI BM")
		isvc := v1beta1testing.MakeInferenceService("isvc-pvc-short-svc", ns.Name).
			NamespacedModel("isvc-pvc-short-llama").
			Runtime("isvc-pvc-short-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("waiting for the Deployment and verifying the PVC mount + MODEL_PATH (short-URI form must produce same wiring)")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(1))
			pod := deps.Items[0].Spec.Template.Spec

			var found *corev1.Volume
			for i := range pod.Volumes {
				if pod.Volumes[i].PersistentVolumeClaim != nil &&
					pod.Volumes[i].PersistentVolumeClaim.ClaimName == pvcName {
					found = &pod.Volumes[i]
					break
				}
			}
			g.Expect(found).NotTo(BeNil(),
				"short URI form must still produce a PVC volume with ClaimName=%q", pvcName)
			g.Expect(found.PersistentVolumeClaim.ReadOnly).To(BeTrue())

			allContainers := append([]corev1.Container{}, pod.Containers...)
			allContainers = append(allContainers, pod.InitContainers...)
			var mountFound, envFound bool
			for _, c := range allContainers {
				for _, vm := range c.VolumeMounts {
					if vm.Name == found.Name && vm.MountPath == constants.ModelDefaultMountPath && vm.SubPath == subPath {
						mountFound = true
					}
				}
				for _, e := range c.Env {
					if e.Name == constants.ModelPathEnvVarKey && e.Value == constants.ModelDefaultMountPath {
						envFound = true
					}
				}
			}
			g.Expect(mountFound).To(BeTrue(),
				"short-URI mount path must equal long-URI: name=%s mount=%s sub=%s",
				found.Name, constants.ModelDefaultMountPath, subPath)
			g.Expect(envFound).To(BeTrue(),
				"%s=%s must be set on the runner container",
				constants.ModelPathEnvVarKey, constants.ModelDefaultMountPath)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		_ = k8sClient.Delete(suiteCtx, bm)
	})
})

// containerMountsForDebug summarizes container name + mounts in a
// compact form for failure messages.
func containerMountsForDebug(cs []corev1.Container) []map[string]any {
	out := make([]map[string]any, 0, len(cs))
	for _, c := range cs {
		mounts := make([]map[string]string, 0, len(c.VolumeMounts))
		for _, m := range c.VolumeMounts {
			mounts = append(mounts, map[string]string{
				"name": m.Name, "mount": m.MountPath, "sub": m.SubPath,
			})
		}
		out = append(out, map[string]any{"name": c.Name, "mounts": mounts})
	}
	return out
}
