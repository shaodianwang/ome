package kindfix

import (
	"context"

	"github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// StandardModelStorageURI is the OCI model URI the in-process KIND suites point
// their ClusterBaseModel at. It is a placeholder — no real model is fetched
// under KIND (the fake model-cache in StandardOMESetup answers every source),
// so only the URI shape has to be valid. The perm suite passes its own URI.
const StandardModelStorageURI = "oci://n/test/b/models/o/llama-7b"

// StandardClusterBaseModel builds the llama-shape ClusterBaseModel the KIND
// suites share: safetensors format, LlamaForCausalLM architecture, Transformers
// framework — the shape StandardClusterServingRuntime auto-selects for.
// storageURI is a parameter because each suite uses its own placeholder URI.
func StandardClusterBaseModel(name, storageURI string) *v1beta1.ClusterBaseModel {
	return v1beta1testing.MakeClusterBaseModel(name).
		StorageURI(storageURI).
		ModelFormat("safetensors").
		ModelType("llama").
		ModelArchitecture("LlamaForCausalLM").
		ModelFramework("Transformers").
		Obj()
}

// StandardClusterServingRuntime builds the ClusterServingRuntime that
// auto-selects for StandardClusterBaseModel: it supports the safetensors /
// LlamaForCausalLM / Transformers shape with autoSelect on and priority 10.
func StandardClusterServingRuntime(name string) *v1beta1.ClusterServingRuntime {
	return v1beta1testing.MakeClusterServingRuntime(name).
		SupportsModelFormat("safetensors", true, 10).
		WithLastFormatArchitecture("LlamaForCausalLM").
		Obj()
}

// ApplyModelRuntimeOpts adjusts ApplyModelRuntime for the two cluster setups
// the KIND suites use.
type ApplyModelRuntimeOpts struct {
	// IdempotentCreate tolerates an AlreadyExists error on the CBM/CSR create.
	// The perm suite creates the shared CBM/CSR once and reuses them across
	// parallel processes, so a repeat create must not fail. The per-test suites
	// create uniquely-named resources and leave this false.
	IdempotentCreate bool

	// CachedReader, when set, is polled until both the CBM and CSR are readable
	// through it before ApplyModelRuntime returns. Pass the in-process manager's
	// cached client (kindutil.Suite().Manager.GetClient()) so the ISVC is not
	// created before the reconciler's cache has the model + runtime — otherwise
	// the first reconcile reports "ClusterBaseModel not found" and backs off.
	// The perm suite runs the manager out of process and has no in-process
	// cache, so it leaves this nil.
	CachedReader client.Reader
}

// ApplyModelRuntime creates the ClusterBaseModel then the ClusterServingRuntime,
// stamps the model-Ready label on every node (no model-agent runs under KIND, so
// the scheduler would otherwise reject the pods on node affinity), and — when a
// CachedReader is given — waits for the controller cache to observe both before
// returning. It does NOT create the ISVC: the caller owns that so each spec
// controls timing and any pre-create mutation. Assertions go through g because
// this package is imported by test code rather than being a test itself.
func ApplyModelRuntime(
	ctx context.Context,
	g gomega.Gomega,
	c client.Client,
	cbm *v1beta1.ClusterBaseModel,
	csr *v1beta1.ClusterServingRuntime,
	opts ApplyModelRuntimeOpts,
) {
	create := func(obj client.Object, what string) {
		err := c.Create(ctx, obj)
		if opts.IdempotentCreate && apierrors.IsAlreadyExists(err) {
			return
		}
		g.ExpectWithOffset(1, err).To(gomega.Succeed(), what)
	}
	create(cbm, "create ClusterBaseModel")
	create(csr, "create ClusterServingRuntime")

	g.ExpectWithOffset(1, kindutil.LabelClusterBaseModelReady(ctx, c, cbm.Name)).
		To(gomega.Succeed(), "label nodes model-Ready for "+cbm.Name)

	if opts.CachedReader != nil {
		gomega.EventuallyWithOffset(1, func(gg gomega.Gomega) {
			gg.Expect(opts.CachedReader.Get(ctx, client.ObjectKey{Name: cbm.Name}, &v1beta1.ClusterBaseModel{})).To(gomega.Succeed())
			gg.Expect(opts.CachedReader.Get(ctx, client.ObjectKey{Name: csr.Name}, &v1beta1.ClusterServingRuntime{})).To(gomega.Succeed())
		}).WithTimeout(MediumTimeout).WithPolling(PollInterval).Should(gomega.Succeed())
	}
}

// CleanupModelRuntime deletes the ISVC first, waits for the apiserver to finish
// removing it (after its finalizer drops), then deletes the CBM and CSR. The
// order matters: deleting the CBM while the ISVC still exists makes the next
// reconcile report "ClusterBaseModel not found" and back off, which starves the
// work queue for later specs. All arguments are nil-safe. It blocks on an
// Eventually, so the caller runs it from a ginkgo.DeferCleanup (this package
// does not import ginkgo).
func CleanupModelRuntime(
	ctx context.Context,
	c client.Client,
	cbm *v1beta1.ClusterBaseModel,
	csr *v1beta1.ClusterServingRuntime,
	isvc *v1beta1.InferenceService,
) {
	if isvc != nil {
		_ = c.Delete(ctx, isvc)
		key := client.ObjectKeyFromObject(isvc)
		gomega.Eventually(func() bool {
			return apierrors.IsNotFound(c.Get(ctx, key, &v1beta1.InferenceService{}))
		}).WithTimeout(MediumTimeout).WithPolling(PollInterval).Should(gomega.BeTrue(),
			"ISVC %s/%s should be fully deleted before CBM/CSR cleanup", isvc.Namespace, isvc.Name)
	}
	if cbm != nil {
		_ = c.Delete(ctx, cbm)
	}
	if csr != nil {
		_ = c.Delete(ctx, csr)
	}
}
