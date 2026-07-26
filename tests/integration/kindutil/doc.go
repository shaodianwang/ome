// Package kindutil provides the integration-test harness that runs
// OME controllers against a real KIND cluster instead of envtest.
//
// envtest is excellent for webhook admission and pure-function
// controller tests but ships no kubelet, scheduler, or container
// runtime. Behavior-style tests that sample pod lifecycle during a
// rollout (coordination and OMENative pod-status specs) would need a
// hand-rolled PodKubeletSimulator (tests/integration/util/simulators.go)
// to fake what kubelet would have done. The simulator has known
// structural limits — it can't model probe failures, ImagePullBackOff,
// scheduling pressure, or the sub-second pod-Ready hand-off that
// Sequential coordination gating relies on.
//
// kindutil uses a real KIND cluster instead:
//
//   - one cluster per test package (Ginkgo suite); ~25s spinup
//   - in-process controller-runtime manager (same shape as envtest);
//     iteration loop unchanged from envtest's framework package
//   - tiny fake-serving container image built locally and loaded into
//     the cluster's containerd via `kind load docker-image` — no
//     registry, no auth, no network
//   - cluster reuse via KIND_KEEP_CLUSTER=1 so iteration is <30s
//
// Test authors port from envtest by swapping their TestXxx entry
// point from framework.RunSuite to kindutil.RunKindSuite. The
// ManagerSetup callback shape is identical so reconciler wiring
// doesn't change.
//
// Webhook admission tests stay on envtest (KIND carries no
// cert-manager or webhook Service wiring).
package kindutil
