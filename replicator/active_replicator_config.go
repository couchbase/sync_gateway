package replicator

import (
	"net/url"

	"github.com/couchbase/sync_gateway/db"
)

type ActiveReplicatorDirection uint8

const (
	ActiveReplicatorTypePushAndPull ActiveReplicatorDirection = iota
	ActiveReplicatorTypePush
	ActiveReplicatorTypePull
)

// ActiveReplicatorConfig controls the behaviour of the active replicator.
// TODO: This might be replaced with ReplicatorConfig in the future.
type ActiveReplicatorConfig struct {
	ID string
	// Type of replication: PushAndPull, Push, or Pull
	Direction ActiveReplicatorDirection
	// PassiveDB represents the full Sync Gateway URL, including database path, and basic auth credentials
	PassiveDB *url.URL
	// ActiveDB is a reference to the active database context
	ActiveDB *db.Database
	// CheckpointInterval controls many revisions to process before storing a checkpoint
	CheckpointInterval uint16
	// ChangesBatchSize controls how many revisions may be batched per changes message
	ChangesBatchSize uint16
	Continuous       bool
	Since            uint64
	Filter           string
}
