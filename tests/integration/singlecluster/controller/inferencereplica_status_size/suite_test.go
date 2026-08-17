package inferencereplicastatussize

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

const testNamespace = "ir-status-size"

var (
	suiteCtx     context.Context
	suiteFwk     *framework.Framework
	suiteStarted bool
	suiteCfg     *rest.Config
	k8sClient    client.Client
	statusWrites *statusRequestCapture
)

func TestInferenceReplicaStatusSize(t *testing.T) {
	util.RunSuite(t, "InferenceReplica Status Size Integration Suite")
}

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	suiteFwk = &framework.Framework{}
	cfg, err := suiteFwk.Init()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	suiteStarted = true

	statusWrites = &statusRequestCapture{}
	suiteCfg = rest.CopyConfig(cfg)
	suiteCfg.Wrap(statusWrites.wrap)

	suiteCtx, k8sClient, err = suiteFwk.SetupClient(suiteCfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	err = k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNamespace},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.AfterSuite(func() {
	if suiteStarted {
		_ = suiteFwk.Teardown()
	}
})

type capturedStatusRequest struct {
	body          []byte
	contentLength int64
	contentType   string
	statusCode    int
}

type statusRequestCapture struct {
	mu       sync.Mutex
	armed    bool
	requests []capturedStatusRequest
}

func (c *statusRequestCapture) begin() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = true
	c.requests = nil
}

func (c *statusRequestCapture) finish() []capturedStatusRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = false
	out := make([]capturedStatusRequest, len(c.requests))
	copy(out, c.requests)
	return out
}

func (c *statusRequestCapture) wrap(next http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		c.mu.Lock()
		capture := c.armed && isInferenceReplicaStatusUpdate(req)
		c.mu.Unlock()
		if !capture {
			return next.RoundTrip(req)
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("read status request body: %w", err)
		}
		_ = req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		}

		resp, roundTripErr := next.RoundTrip(req)
		statusCode := 0
		if resp != nil {
			statusCode = resp.StatusCode
		}
		c.mu.Lock()
		c.requests = append(c.requests, capturedStatusRequest{
			body:          append([]byte(nil), body...),
			contentLength: req.ContentLength,
			contentType:   req.Header.Get("Content-Type"),
			statusCode:    statusCode,
		})
		c.mu.Unlock()
		return resp, roundTripErr
	})
}

func isInferenceReplicaStatusUpdate(req *http.Request) bool {
	if req == nil || req.URL == nil || req.Method != http.MethodPut {
		return false
	}
	path := req.URL.Path
	return strings.HasPrefix(path, "/apis/ome.io/v1beta1/namespaces/") &&
		strings.Contains(path, "/inferencereplicas/") &&
		strings.HasSuffix(path, "/status")
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
