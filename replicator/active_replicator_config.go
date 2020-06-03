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
	// Filter is a predetermined filter name (e.g. sync_gateway/bychannel)
	Filter string
	// FilterChannels are a set of channels to be used by the sync_gateway/bychannel filter.
	FilterChannels []string
	// DocIDs limits the changes to only those doc IDs specified.
	DocIDs []string
	// ActiveOnly when true prevents changes being sent for tombstones on the initial replication.
	ActiveOnly bool
	// Since represents the sequence we're going to perform the replication from.
	Since db.SequenceID
	// ChangesBatchSize controls how many revisions may be batched per changes message.
	ChangesBatchSize uint16
	// Direction, otherwise known as the type of replication: PushAndPull, Push, or Pull.
	Direction ActiveReplicatorDirection
	// Continuous specifies whether the replication should be continuous or one-shot.
	Continuous bool
	// PassiveDBURL represents the full Sync Gateway URL, including database path, and basic auth credentials of the target.
	PassiveDBURL *url.URL

	// ActiveDB is a reference to the active database context.
	ActiveDB *db.Database
}
