package status

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func makePodWithWaiting(creationOffset time.Duration, containerReason string, initReason string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-pod",
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now().Add(creationOffset)),
		},
	}
	if containerReason != "" {
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{
			{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: containerReason}},
			},
		}
	}
	if initReason != "" {
		pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
			{
				Name:  "init",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: initReason}},
			},
		}
	}
	return pod
}

func TestPodStuckPullFailure_NilPodReturnsFalse(t *testing.T) {
	ok, reason := PodStuckPullFailure(nil, time.Now(), time.Minute)
	if ok {
		t.Errorf("nil pod should return false; got ok=%v reason=%q", ok, reason)
	}
}

func TestPodStuckPullFailure_NoCreationTimestamp(t *testing.T) {
	pod := &corev1.Pod{}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "main", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}},
	}
	ok, _ := PodStuckPullFailure(pod, time.Now(), time.Minute)
	if ok {
		t.Errorf("pod without CreationTimestamp should return false; got ok=%v", ok)
	}
}

func TestPodStuckPullFailure_WithinGraceReturnsFalse(t *testing.T) {
	// Pod created 10s ago, grace = 60s → too early to fire.
	pod := makePodWithWaiting(-10*time.Second, "ImagePullBackOff", "")
	ok, _ := PodStuckPullFailure(pod, time.Now(), 60*time.Second)
	if ok {
		t.Errorf("pod within grace window should return false; got ok=%v", ok)
	}
}

func TestPodStuckPullFailure_ImagePullBackOffPastGrace(t *testing.T) {
	// Pod created 2m ago, grace = 60s → past grace, fires.
	pod := makePodWithWaiting(-2*time.Minute, "ImagePullBackOff", "")
	ok, reason := PodStuckPullFailure(pod, time.Now(), 60*time.Second)
	if !ok {
		t.Fatalf("ImagePullBackOff past grace should fire; got ok=%v", ok)
	}
	if reason != "ImagePullBackOff" {
		t.Errorf("reason: got %q want ImagePullBackOff", reason)
	}
}

func TestPodStuckPullFailure_ErrImagePullPastGrace(t *testing.T) {
	pod := makePodWithWaiting(-2*time.Minute, "ErrImagePull", "")
	ok, reason := PodStuckPullFailure(pod, time.Now(), 60*time.Second)
	if !ok || reason != "ErrImagePull" {
		t.Errorf("ErrImagePull past grace should fire; got ok=%v reason=%q", ok, reason)
	}
}

func TestPodStuckPullFailure_InvalidImageNamePastGrace(t *testing.T) {
	pod := makePodWithWaiting(-2*time.Minute, "InvalidImageName", "")
	ok, reason := PodStuckPullFailure(pod, time.Now(), 60*time.Second)
	if !ok || reason != "InvalidImageName" {
		t.Errorf("InvalidImageName past grace should fire; got ok=%v reason=%q", ok, reason)
	}
}

func TestPodStuckPullFailure_CreateContainerErrorPastGrace(t *testing.T) {
	pod := makePodWithWaiting(-2*time.Minute, "CreateContainerError", "")
	ok, reason := PodStuckPullFailure(pod, time.Now(), 60*time.Second)
	if !ok || reason != "CreateContainerError" {
		t.Errorf("CreateContainerError past grace should fire; got ok=%v reason=%q", ok, reason)
	}
}

func TestPodStuckPullFailure_InitContainerImagePullBackOff(t *testing.T) {
	// Init container with ImagePullBackOff; main container has no status
	// (kubelet doesn't start regular containers until init succeeds).
	pod := makePodWithWaiting(-2*time.Minute, "", "ImagePullBackOff")
	ok, reason := PodStuckPullFailure(pod, time.Now(), 60*time.Second)
	if !ok || reason != "ImagePullBackOff" {
		t.Errorf("init container ImagePullBackOff past grace should fire; got ok=%v reason=%q", ok, reason)
	}
}

func TestPodStuckPullFailure_BenignWaitingReasonsDoNotFire(t *testing.T) {
	// Transient/benign waiting reasons that should NOT trigger escalation.
	for _, reason := range []string{"PodInitializing", "ContainerCreating", "ImagePulling"} {
		pod := makePodWithWaiting(-2*time.Minute, reason, "")
		ok, _ := PodStuckPullFailure(pod, time.Now(), 60*time.Second)
		if ok {
			t.Errorf("benign waiting reason %q should NOT fire; got ok=%v", reason, ok)
		}
	}
}

func TestPodStuckPullFailure_RunningContainerDoesNotFire(t *testing.T) {
	// Container in Running state — no Waiting struct — must not fire.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Minute)),
		},
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "main", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
	}
	ok, _ := PodStuckPullFailure(pod, time.Now(), 60*time.Second)
	if ok {
		t.Errorf("running container should not fire; got ok=%v", ok)
	}
}

func TestPodStuckPullFailure_MultipleContainersOneFailing(t *testing.T) {
	// One container running, another in ImagePullBackOff. Should fire on
	// the failing container (any container failing wedges the pod).
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Minute)),
		},
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "main", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
		{Name: "sidecar", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}},
	}
	ok, reason := PodStuckPullFailure(pod, time.Now(), 60*time.Second)
	if !ok || reason != "ImagePullBackOff" {
		t.Errorf("mixed pod with one ImagePullBackOff container should fire; got ok=%v reason=%q", ok, reason)
	}
}
