package coordination

import "k8s.io/apimachinery/pkg/util/intstr"

// ptrIntOrStringFromInt returns a pointer to an IntOrString carrying
// the supplied int value. Helper used by pacing defaulters that need
// to set "0" without taking the address of a local literal.
func ptrIntOrStringFromInt(v int) *intstr.IntOrString {
	out := intstr.FromInt(v)
	return &out
}

// ptrIntOrStringFromString returns a pointer to an IntOrString carrying
// the supplied percentage string (e.g. "25%"). Helper used by pacing
// defaulters.
func ptrIntOrStringFromString(v string) *intstr.IntOrString {
	out := intstr.FromString(v)
	return &out
}
