package replicator

import (
	"github.com/ome-projects/ome/internal/ome-agent/replica/common"
)

type Replicator interface {
	Replicate(objects []common.ReplicationObject) error
}
