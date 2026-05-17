// Package fake provides a test double for sharded.Client.
// Implements the interface via per-method funcs so tests can script
// behavior without a real httptest server.
package fake

import (
	"context"
	"errors"
	"sync"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
)

// ErrNotImplemented is returned by fake methods that the test did not
// stub. Failing loudly is preferable to returning a zero value that
// looks like a successful no-op.
var ErrNotImplemented = errors.New("fake sharded client method not implemented")

// Client is a test double for sharded.Client.
type Client struct {
	CheckSourceFunc   func(context.Context, sharded.Source) (*sharded.SourceStatus, error)
	InspectSourceFunc func(context.Context, sharded.Source) (*sharded.SourceInspection, error)

	mu                 sync.Mutex
	checkSourceCalls   []sharded.Source
	inspectSourceCalls []sharded.Source
}

var _ sharded.Client = (*Client)(nil)

func (c *Client) CheckSource(ctx context.Context, source sharded.Source) (*sharded.SourceStatus, error) {
	c.mu.Lock()
	c.checkSourceCalls = append(c.checkSourceCalls, source)
	c.mu.Unlock()

	if c.CheckSourceFunc == nil {
		return nil, ErrNotImplemented
	}
	return c.CheckSourceFunc(ctx, source)
}

func (c *Client) InspectSource(ctx context.Context, source sharded.Source) (*sharded.SourceInspection, error) {
	c.mu.Lock()
	c.inspectSourceCalls = append(c.inspectSourceCalls, source)
	c.mu.Unlock()

	if c.InspectSourceFunc == nil {
		return nil, ErrNotImplemented
	}
	return c.InspectSourceFunc(ctx, source)
}

func (c *Client) CheckSourceCalls() []sharded.Source {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sharded.Source(nil), c.checkSourceCalls...)
}

func (c *Client) InspectSourceCalls() []sharded.Source {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sharded.Source(nil), c.inspectSourceCalls...)
}
