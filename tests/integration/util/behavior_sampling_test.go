package util

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// A pod leaving with a grace window is a graceful delete; one leaving
// with a grace of zero is a force delete caught between the API server's
// two writes, and one with no deletion timestamp is not leaving at all.
func TestGracefulDeleteInFlight(t *testing.T) {
	stamp := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	for _, tc := range []struct {
		name  string
		pod   *corev1.Pod
		wantS bool
	}{
		{name: "nil pod"},
		{name: "no deletion", pod: &corev1.Pod{}},
		{name: "force delete, grace zero", pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			DeletionTimestamp: &stamp, DeletionGracePeriodSeconds: ptr.To(int64(0))}}},
		{name: "graceful delete, grace window", wantS: true, pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			DeletionTimestamp: &stamp, DeletionGracePeriodSeconds: ptr.To(int64(30))}}},
		{name: "graceful delete, grace not recorded", wantS: true, pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			DeletionTimestamp: &stamp}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := GracefulDeleteInFlight(tc.pod); got != tc.wantS {
				t.Fatalf("GracefulDeleteInFlight = %v, want %v", got, tc.wantS)
			}
		})
	}
}
