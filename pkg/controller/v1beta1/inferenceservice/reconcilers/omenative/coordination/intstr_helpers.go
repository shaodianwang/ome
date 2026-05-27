package coordination

import "k8s.io/apimachinery/pkg/util/intstr"

// ptrIntOrStringFromString returns a pointer to an IntOrString carrying
// the supplied percentage string (e.g. "25%"). Helper used by pacing
// defaulters.
func ptrIntOrStringFromString(v string) *intstr.IntOrString {
	out := intstr.FromString(v)
	return &out
}
