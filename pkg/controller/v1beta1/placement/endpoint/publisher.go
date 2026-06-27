// Package endpoint programs ONE concrete global-traffic backend so the global
// host of a multi-cluster InferenceService resolves to whichever workload
// cluster currently wins the placement race.
//
// The control-plane fan-out controller (package placement) already records the
// winner and the winner's externally-addressable URL in
// status.placement.{cluster,endpoint}. That is status-only: an external LB has
// to consume it. This package closes that gap by actually programming a backend
// — the Gateway API HTTPRoute backend (GatewayAPIPublisher) is the first
// concrete implementation; the EndpointPublisher interface lets DNS/GSLB
// backends be added later without touching the reconciler.
package endpoint

import (
	"context"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Target is the resolved publication intent for one InferenceService: the global
// host to program and the winning cluster's ingress endpoint that host must
// resolve to. Built by the reconciler from status.placement and the Config; the
// EndpointPublisher backend translates it into its own resource(s).
type Target struct {
	// GlobalHost is the externally-addressable hostname the publisher programs
	// (e.g. "my-svc.global.example"). Rendered from Config + the ISVC, never a
	// baked-in literal.
	GlobalHost string

	// Cluster is the WorkloadCluster that currently wins the placement race
	// (status.placement.cluster). Informational for the backend (labels,
	// logging); the route itself targets BackendHost.
	Cluster string

	// BackendHost is the winning cluster's ingress hostname that the global host
	// must resolve to — the host of status.placement.endpoint. A bare hostname
	// (no scheme/port); the backend supplies the port from Config.
	BackendHost string
}

// EndpointPublisher programs a single global-traffic backend so the global host
// of a multi-cluster InferenceService points at the winning cluster's ingress,
// repoints it on re-placement, and tears it down when the service is no longer
// placed. Implementations are backend-specific (Gateway API HTTPRoute today;
// DNS/GSLB later) and MUST be idempotent: Publish for an unchanged Target is a
// no-op, and Unpublish for an already-clean service is a no-op.
type EndpointPublisher interface {
	// Publish ensures the backend routes target.GlobalHost to
	// target.BackendHost for isvc, creating or repointing as needed.
	Publish(ctx context.Context, isvc *v1beta1.InferenceService, target Target) error

	// Unpublish removes any backend resources this publisher created for isvc.
	// Safe to call when nothing was ever published.
	Unpublish(ctx context.Context, isvc *v1beta1.InferenceService) error

	// Name identifies the backend for logging/metrics (e.g. "GatewayAPI").
	Name() string
}
