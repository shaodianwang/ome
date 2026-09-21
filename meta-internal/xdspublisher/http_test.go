//go:build meta_internal

package xdspublisher

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testHTTPPublisher(server *httptest.Server) *httpClusterWeightPublisher {
	return &httpClusterWeightPublisher{
		client:      server.Client(),
		urlTemplate: server.URL + "/api/v1/clusters/{cluster}/weight",
		createIfMissing: createTierSettings{
			Oncall: "inference",
			ACL:    stringPointer("cluster-writers"),
		},
	}
}

func TestHTTPPublisherPublishesWeight(t *testing.T) {
	var method, path, contentType string
	var body publishWeightRequest
	var decodeErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		path = r.URL.EscapedPath()
		contentType = r.Header.Get("Content-Type")
		decodeErr = json.NewDecoder(r.Body).Decode(&body)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	err := testHTTPPublisher(server).PublishWeight(context.Background(), "scope/cluster", 0.37)
	require.NoError(t, err)
	require.NoError(t, decodeErr)
	assert.Equal(t, http.MethodPut, method)
	assert.Equal(t, "/api/v1/clusters/scope%2Fcluster/weight", path)
	assert.Equal(t, "application/json", contentType)
	assert.Equal(t, 0.37, body.Weight)
	assert.Equal(t, "inference", body.CreateIfMissing.Oncall)
	require.NotNil(t, body.CreateIfMissing.ACL)
	assert.Equal(t, "cluster-writers", *body.CreateIfMissing.ACL)
}

func TestHTTPPublisherOmitsUnsetACL(t *testing.T) {
	var body map[string]any
	var decodeErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decodeErr = json.NewDecoder(r.Body).Decode(&body)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	publisher := testHTTPPublisher(server)
	publisher.createIfMissing.ACL = nil

	require.NoError(t, publisher.PublishWeight(context.Background(), "cluster", 1))
	require.NoError(t, decodeErr)
	createIfMissing, ok := body["createIfMissing"].(map[string]any)
	require.True(t, ok)
	assert.NotContains(t, createIfMissing, "acl")
}

func TestHTTPPublisherClearsWeight(t *testing.T) {
	var method, path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		path = r.URL.EscapedPath()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	require.NoError(t, testHTTPPublisher(server).ClearWeight(context.Background(), "cluster"))
	assert.Equal(t, http.MethodDelete, method)
	assert.Equal(t, "/api/v1/clusters/cluster/weight", path)
}

func TestHTTPPublisherSurfacesAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"STORAGE_FAILURE","message":"SMC unavailable"}`))
	}))
	t.Cleanup(server.Close)

	err := testHTTPPublisher(server).PublishWeight(context.Background(), "cluster", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 503")
	assert.Contains(t, err.Error(), "STORAGE_FAILURE")
	assert.Contains(t, err.Error(), "SMC unavailable")
}

func TestHTTPPublisherHonorsContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(time.Second)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := testHTTPPublisher(server).ClearWeight(ctx, "cluster")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestNewHTTPPublisherUsesConfiguredTLS(t *testing.T) {
	protocolMajor := 0
	peerCertificates := 0
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protocolMajor = r.ProtoMajor
		peerCertificates = len(r.TLS.PeerCertificates)
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAnyClientCert}
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	dir := t.TempDir()
	certFile := filepath.Join(dir, "client.pem")
	keyFile := filepath.Join(dir, "client-key.pem")
	caFile := filepath.Join(dir, "ca.pem")
	certificate := server.TLS.Certificates[0]
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})
	keyDER, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
	require.NoError(t, os.WriteFile(caFile, certPEM, 0o600))

	publisher, err := newHTTPClusterWeightPublisher(config{
		urlTemplate:    server.URL + "/api/v1/clusters/{cluster}/weight",
		requestTimeout: time.Second,
		tlsCertFile:    certFile,
		tlsKeyFile:     keyFile,
		tlsCAFile:      caFile,
		oncall:         "inference",
	})
	require.NoError(t, err)
	require.NoError(t, publisher.PublishWeight(context.Background(), "cluster", 2))
	assert.Equal(t, 2, protocolMajor)
	assert.NotZero(t, peerCertificates)

	require.NoError(t, os.WriteFile(keyFile, []byte("rotating"), 0o600))
	publisher.(*httpClusterWeightPublisher).client.CloseIdleConnections()
	err = publisher.PublishWeight(context.Background(), "cluster", 3)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load xDS automation client certificate")
}

func stringPointer(value string) *string {
	return &value
}
