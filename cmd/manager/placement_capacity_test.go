package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
)

func TestCapacityPlacementWiring(t *testing.T) {
	for _, tt := range []struct {
		name, config string
		want         *placement.CapacityConfig
	}{
		{name: "absent"},
		{name: "explicit", config: `{"placement":{"capacity":{"rootName":"capacity-root","memberOperatorNamespace":"operator-system","maxAge":"5m","stabilityWindow":"1m","refreshInterval":"10s"}}}`, want: &placement.CapacityConfig{RootName: "capacity-root", MemberOperatorNamespace: "operator-system", MaxAge: 5 * time.Minute, StabilityWindow: time.Minute, RefreshInterval: 10 * time.Second}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := mcWiringFromJSON(t, tt.config)
			if diff := cmp.Diff(tt.want, got.capacity); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
