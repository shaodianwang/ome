package workloadcluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
)

// stubClient is a minimal SelectivelyCachingClient sentinel for identity checks.
type stubClient struct {
	SelectivelyCachingClient
	id string
}

func newTestManager() (*Manager, *[]string) {
	var canceled []string
	m := NewManager(runtime.NewScheme())
	m.newClient = func(_ context.Context, raw []byte, _ *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		id := string(raw)
		return &stubClient{id: id}, func() { canceled = append(canceled, id) }, nil
	}
	return m, &canceled
}

func TestManager_ConnectThenClientFor(t *testing.T) {
	m, _ := newTestManager()
	require.NoError(t, m.Connect(context.Background(), "c1", []byte("kubeconfA")))
	got, ok := m.ClientFor("c1")
	require.True(t, ok)
	assert.Equal(t, "kubeconfA", got.(*stubClient).id)
}

func TestManager_ReconnectOnlyOnConfigChange(t *testing.T) {
	m, canceled := newTestManager()
	require.NoError(t, m.Connect(context.Background(), "c1", []byte("A")))
	require.NoError(t, m.Connect(context.Background(), "c1", []byte("A")))
	assert.Empty(t, *canceled, "unchanged kubeconfig must not rebuild")
	require.NoError(t, m.Connect(context.Background(), "c1", []byte("B")))
	assert.Equal(t, []string{"A"}, *canceled, "changed kubeconfig must dispose the old client")
	got, _ := m.ClientFor("c1")
	assert.Equal(t, "B", got.(*stubClient).id)
}

func TestManager_Disconnect(t *testing.T) {
	m, canceled := newTestManager()
	require.NoError(t, m.Connect(context.Background(), "c1", []byte("A")))
	m.Disconnect("c1")
	_, ok := m.ClientFor("c1")
	assert.False(t, ok, "Disconnect must remove the client")
	assert.Equal(t, []string{"A"}, *canceled, "Disconnect must cancel the watch context")
}

func TestManager_ClientForUnknown(t *testing.T) {
	m, _ := newTestManager()
	_, ok := m.ClientFor("nope")
	assert.False(t, ok)
}
