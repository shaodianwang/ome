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

package util

import (
	"testing"

	partitiontreev1 "tpu.google.com/slice-scheduler/apis/partition/v1beta1"
)

func TestIsPartitionAvailable(t *testing.T) {
	tests := []struct {
		name string
		info partitiontreev1.PartitionInfo
		want bool
	}{
		{
			name: "HEALTHY state with no occupancy and no sliceId is available",
			info: partitiontreev1.PartitionInfo{State: partitiontreev1.PartitionStateHealthy},
			want: true,
		},
		{
			name: "HEALTHY state with OCCUPIED occupancy is not available",
			info: partitiontreev1.PartitionInfo{
				State:     partitiontreev1.PartitionStateHealthy,
				Occupancy: partitiontreev1.OccupancyOccupied,
			},
			want: false,
		},
		{
			name: "HEALTHY state with sliceId is not available",
			info: partitiontreev1.PartitionInfo{
				State:   partitiontreev1.PartitionStateHealthy,
				SliceId: "some-slice",
			},
			want: false,
		},
		{
			name: "BROKEN state is not available",
			info: partitiontreev1.PartitionInfo{State: partitiontreev1.PartitionStateUnhealthy},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPartitionAvailable(&tt.info); got != tt.want {
				t.Errorf("IsPartitionAvailable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsPartitionOccupied(t *testing.T) {
	tests := []struct {
		name string
		info partitiontreev1.PartitionInfo
		want bool
	}{
		{
			name: "OCCUPIED occupancy is occupied",
			info: partitiontreev1.PartitionInfo{
				Occupancy: partitiontreev1.OccupancyOccupied,
			},
			want: true,
		},
		{
			name: "EMPTY occupancy is not occupied",
			info: partitiontreev1.PartitionInfo{
				Occupancy: partitiontreev1.OccupancyEmpty,
			},
			want: false,
		},
		{
			name: "No occupancy set is not occupied",
			info: partitiontreev1.PartitionInfo{},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPartitionOccupied(&tt.info); got != tt.want {
				t.Errorf("IsPartitionOccupied() = %v, want %v", got, tt.want)
			}
		})
	}
}
