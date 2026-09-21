//go:build meta_internal

package xdspublisher

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

const maxErrorResponseBytes = 64 << 10

type createTierSettings struct {
	Oncall string  `json:"oncall"`
	ACL    *string `json:"acl,omitempty"`
}

type publishWeightRequest struct {
	Weight          float64            `json:"weight"`
	CreateIfMissing createTierSettings `json:"createIfMissing"`
}

type apiErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type httpClusterWeightPublisher struct {
	client          *http.Client
	urlTemplate     string
	createIfMissing createTierSettings
}

func newHTTPClusterWeightPublisher(cfg config) (ClusterWeightPublisher, error) {
	loadClientCertificate := func() (*tls.Certificate, error) {
		certificate, err := tls.LoadX509KeyPair(cfg.tlsCertFile, cfg.tlsKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load xDS automation client certificate: %w", err)
		}
		return &certificate, nil
	}
	if _, err := loadClientCertificate(); err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(cfg.tlsCAFile)
	if err != nil {
		return nil, fmt.Errorf("read xDS automation CA certificate: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("xDS automation CA certificate contains no certificates")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	// The remote proxy terminates the workload's mutual TLS connection. An
	// ambient HTTP proxy would terminate that connection under another identity.
	transport.Proxy = nil
	transport.ForceAttemptHTTP2 = true
	transport.TLSClientConfig = &tls.Config{
		MinVersion:           tls.VersionTLS12,
		RootCAs:              roots,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return loadClientCertificate() },
	}

	return &httpClusterWeightPublisher{
		client: &http.Client{
			Transport: transport,
			Timeout:   cfg.requestTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		urlTemplate: cfg.urlTemplate,
		createIfMissing: createTierSettings{
			Oncall: cfg.oncall,
			ACL:    cfg.acl,
		},
	}, nil
}

func (p *httpClusterWeightPublisher) PublishWeight(
	ctx context.Context,
	cluster string,
	weight float64,
) error {
	body, err := json.Marshal(publishWeightRequest{
		Weight:          weight,
		CreateIfMissing: p.createIfMissing,
	})
	if err != nil {
		return fmt.Errorf("encode cluster weight request: %w", err)
	}
	return p.do(ctx, http.MethodPut, cluster, body)
}

func (p *httpClusterWeightPublisher) ClearWeight(ctx context.Context, cluster string) error {
	return p.do(ctx, http.MethodDelete, cluster, nil)
}

func (p *httpClusterWeightPublisher) do(
	ctx context.Context,
	method string,
	cluster string,
	body []byte,
) error {
	requestURL := strings.Replace(
		p.urlTemplate,
		clusterURLPlaceholder,
		url.PathEscape(cluster),
		1,
	)
	request, err := http.NewRequestWithContext(ctx, method, requestURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build xDS automation request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := p.client.Do(request)
	if err != nil {
		return fmt.Errorf("call xDS automation API: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return nil
	}

	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxErrorResponseBytes+1))
	if readErr != nil {
		return fmt.Errorf("xDS automation API returned HTTP %d and its error response could not be read: %w",
			response.StatusCode, readErr)
	}
	if len(responseBody) > maxErrorResponseBytes {
		return fmt.Errorf("xDS automation API returned HTTP %d with an oversized error response",
			response.StatusCode)
	}
	var apiError apiErrorResponse
	if err := json.Unmarshal(responseBody, &apiError); err == nil &&
		(apiError.Code != "" || apiError.Message != "") {
		return fmt.Errorf("xDS automation API returned HTTP %d (%s): %s",
			response.StatusCode, apiError.Code, apiError.Message)
	}
	return fmt.Errorf("xDS automation API returned HTTP %d", response.StatusCode)
}
