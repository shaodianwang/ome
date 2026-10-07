/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pod_reconciler

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParsePodIndex(t *testing.T) {
	tests := []struct {
		name    string
		pod     *corev1.Pod
		want    int
		wantErr bool
	}{
		{
			name: "StatefulSet-style pod name",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-0"},
			},
			want: 0,
		},
		{
			name: "StatefulSet-style pod name with multi-digits",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "my-tpu-pod-10"},
			},
			want: 10,
		},
		{
			name: "Job completion index annotation",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "job-abc-123",
					Annotations: map[string]string{
						JobCompletionIndexAnnotation: "5",
					},
				},
			},
			want: 5,
		},
		{
			name: "Custom worker-id annotation",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "custom-worker",
					Annotations: map[string]string{
						WorkerIdAnnotation: "3",
					},
				},
			},
			want: 3,
		},
		{
			name: "Job completion index takes precedence over name",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pod-10",
					Annotations: map[string]string{
						JobCompletionIndexAnnotation: "2",
					},
				},
			},
			want: 2,
		},
		{
			name: "Invalid pod name",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod"},
			},
			wantErr: true,
		},
		{
			name: "Non-numeric index in name",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-abc"},
			},
			wantErr: true,
		},
		{
			name: "LWS worker index label",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "lws-worker-pod",
					Labels: map[string]string{
						LWSWorkerIndexLabel: "5",
					},
				},
			},
			want: 5,
		},
		{
			name: "StatefulSet pod-index label takes precedence over LWS worker-index",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "lws-pod-0",
					Labels: map[string]string{
						PodIndexLabel:       "2",
						LWSWorkerIndexLabel: "7",
					},
				},
			},
			want: 2,
		},
		{
			name: "LWS worker index label with non-numeric value",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "lws-pod-bad",
					Labels: map[string]string{
						LWSWorkerIndexLabel: "abc",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePodIndex(tt.pod)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePodIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parsePodIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}
