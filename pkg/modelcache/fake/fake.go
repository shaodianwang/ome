package fake

import (
	"context"
	"errors"
	"sync"

	"github.com/ome-projects/ome/pkg/modelcache"
)

var ErrNotImplemented = errors.New("fake model cache client method not implemented")

// Client is a test double for modelcache.Client.
type Client struct {
	CheckSourceFunc      func(context.Context, modelcache.Source) (*modelcache.SourceStatus, error)
	InspectSourceFunc    func(context.Context, modelcache.Source) (*modelcache.SourceInspection, error)
	StartPrewarmFunc     func(context.Context, modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error)
	GetPrewarmStatusFunc func(context.Context, modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error)

	mu                    sync.Mutex
	checkSourceCalls      []modelcache.Source
	inspectSourceCalls    []modelcache.Source
	startPrewarmCalls     []modelcache.PrewarmRequest
	getPrewarmStatusCalls []modelcache.PrewarmOperation
}

var _ modelcache.Client = (*Client)(nil)

func (c *Client) CheckSource(ctx context.Context, source modelcache.Source) (*modelcache.SourceStatus, error) {
	c.mu.Lock()
	c.checkSourceCalls = append(c.checkSourceCalls, source)
	c.mu.Unlock()

	if c.CheckSourceFunc == nil {
		return nil, ErrNotImplemented
	}
	return c.CheckSourceFunc(ctx, source)
}

func (c *Client) InspectSource(ctx context.Context, source modelcache.Source) (*modelcache.SourceInspection, error) {
	c.mu.Lock()
	c.inspectSourceCalls = append(c.inspectSourceCalls, source)
	c.mu.Unlock()

	if c.InspectSourceFunc == nil {
		return nil, ErrNotImplemented
	}
	return c.InspectSourceFunc(ctx, source)
}

func (c *Client) StartPrewarm(ctx context.Context, req modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error) {
	c.mu.Lock()
	c.startPrewarmCalls = append(c.startPrewarmCalls, req)
	c.mu.Unlock()

	if c.StartPrewarmFunc == nil {
		return nil, ErrNotImplemented
	}
	return c.StartPrewarmFunc(ctx, req)
}

func (c *Client) GetPrewarmStatus(ctx context.Context, operation modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
	c.mu.Lock()
	c.getPrewarmStatusCalls = append(c.getPrewarmStatusCalls, operation)
	c.mu.Unlock()

	if c.GetPrewarmStatusFunc == nil {
		return nil, ErrNotImplemented
	}
	return c.GetPrewarmStatusFunc(ctx, operation)
}

func (c *Client) CheckSourceCalls() []modelcache.Source {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]modelcache.Source(nil), c.checkSourceCalls...)
}

func (c *Client) InspectSourceCalls() []modelcache.Source {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]modelcache.Source(nil), c.inspectSourceCalls...)
}

func (c *Client) StartPrewarmCalls() []modelcache.PrewarmRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]modelcache.PrewarmRequest(nil), c.startPrewarmCalls...)
}

func (c *Client) GetPrewarmStatusCalls() []modelcache.PrewarmOperation {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]modelcache.PrewarmOperation(nil), c.getPrewarmStatusCalls...)
}
