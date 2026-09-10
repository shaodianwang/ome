package routing

// Config holds the operator-supplied, config-driven inputs for the routing
// controller. TrafficMap generation carries no host/gateway/port values of its
// own — those belong to the downstream publisher — so the only knob is an
// explicit enable toggle.
//
// +kubebuilder:object:generate=false
type Config struct {
	// Enabled turns on TrafficMap generation. Off by default: the controller is a
	// no-op and reaps any TrafficMap it previously created, so enabling or
	// disabling the feature is reversible without stranding objects.
	Enabled bool
}

// IsEnabled reports whether TrafficMap generation is on. When false the routing
// controller publishes nothing and releases what it already owns.
func (c Config) IsEnabled() bool {
	return c.Enabled
}
