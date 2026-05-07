package fake

import (
	"context"
	"errors"
	"sync"

	"github.com/sgl-project/ome/pkg/modelcache"
)

var ErrNotImplemented = errors.New("fake model cache client method not implemented")

// Client is a test double for modelcache.Client.
type Client struct {
	CheckSourceFunc       func(context.Context, modelcache.Source) (*modelcache.SourceStatus, error)
	ListSourceFunc        func(context.Context, modelcache.Source) ([]modelcache.Object, error)
	GetObjectFunc         func(context.Context, modelcache.ObjectRef) (*modelcache.Object, error)
	ReadMetadataRangeFunc func(context.Context, modelcache.MetadataRangeRequest) ([]byte, error)
	StartPrewarmFunc      func(context.Context, modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error)
	GetPrewarmStatusFunc  func(context.Context, modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error)

	mu                     sync.Mutex
	checkSourceCalls       []modelcache.Source
	listSourceCalls        []modelcache.Source
	getObjectCalls         []modelcache.ObjectRef
	readMetadataRangeCalls []modelcache.MetadataRangeRequest
	startPrewarmCalls      []modelcache.PrewarmRequest
	getPrewarmStatusCalls  []modelcache.PrewarmOperation
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

func (c *Client) ListSource(ctx context.Context, source modelcache.Source) ([]modelcache.Object, error) {
	c.mu.Lock()
	c.listSourceCalls = append(c.listSourceCalls, source)
	c.mu.Unlock()

	if c.ListSourceFunc == nil {
		return nil, ErrNotImplemented
	}
	return c.ListSourceFunc(ctx, source)
}

func (c *Client) GetObject(ctx context.Context, ref modelcache.ObjectRef) (*modelcache.Object, error) {
	c.mu.Lock()
	c.getObjectCalls = append(c.getObjectCalls, ref)
	c.mu.Unlock()

	if c.GetObjectFunc == nil {
		return nil, ErrNotImplemented
	}
	return c.GetObjectFunc(ctx, ref)
}

func (c *Client) ReadMetadataRange(ctx context.Context, req modelcache.MetadataRangeRequest) ([]byte, error) {
	c.mu.Lock()
	c.readMetadataRangeCalls = append(c.readMetadataRangeCalls, req)
	c.mu.Unlock()

	if c.ReadMetadataRangeFunc == nil {
		return nil, ErrNotImplemented
	}
	return c.ReadMetadataRangeFunc(ctx, req)
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

func (c *Client) ListSourceCalls() []modelcache.Source {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]modelcache.Source(nil), c.listSourceCalls...)
}

func (c *Client) GetObjectCalls() []modelcache.ObjectRef {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]modelcache.ObjectRef(nil), c.getObjectCalls...)
}

func (c *Client) ReadMetadataRangeCalls() []modelcache.MetadataRangeRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]modelcache.MetadataRangeRequest(nil), c.readMetadataRangeCalls...)
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
