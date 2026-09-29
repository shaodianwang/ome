package framework

import (
	"errors"
	"testing"
)

// The restart applies only to an apiserver that stopped answering during
// bring-up; a rejected request, or anything else, returns as is.
func TestControlPlaneDropped(t *testing.T) {
	dropped := []string{
		`unable to create CRD "servingruntimes.ome.io": Post "https://127.0.0.1:47605/apis/apiextensions.k8s.io/v1/customresourcedefinitions": http2: client connection lost`,
		`Get "https://127.0.0.1:1/healthz": dial tcp 127.0.0.1:1: connect: connection refused`,
		`read tcp 127.0.0.1:57522->127.0.0.1:33773: read: connection reset by peer`,
		`timeout waiting for process kube-apiserver to start successfully (it may have failed to start, or stopped unexpectedly before becoming ready): context deadline exceeded`,
	}
	for _, msg := range dropped {
		if !controlPlaneDropped(errors.New(msg)) {
			t.Errorf("must restart on %q", msg)
		}
	}
	kept := []string{
		`unable to create CRD "servingruntimes.ome.io": CustomResourceDefinition.apiextensions.k8s.io "servingruntimes.ome.io" is invalid`,
		`unable to find binary kube-apiserver`,
	}
	for _, msg := range kept {
		if controlPlaneDropped(errors.New(msg)) {
			t.Errorf("must not restart on %q", msg)
		}
	}
}
