package replicator

import (
	"net/url"
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
	// // CheckpointInterval controls many revisions to process before storing a checkpoint
	// CheckpointInterval uint16 // Default: 200
	// // ChangesBatchSize controls how many revisions may be batched per changes message
	// ChangesBatchSize uint16 // Default: 200
	// TargetDB represents the full Sync Gateway URL, including database path, and basic auth credentials
	TargetDB *url.URL
}
