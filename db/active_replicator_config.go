package db

import (
	"crypto/sha1"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

type ActiveReplicatorDirection string

const (
	ActiveReplicatorTypePushAndPull ActiveReplicatorDirection = "pushAndPull"
	ActiveReplicatorTypePush        ActiveReplicatorDirection = "push"
	ActiveReplicatorTypePull        ActiveReplicatorDirection = "pull"
)

func (d ActiveReplicatorDirection) IsValid() bool {
	switch d {
	case ActiveReplicatorTypePushAndPull, ActiveReplicatorTypePush, ActiveReplicatorTypePull:
		return true
	default:
		return false
	}
}

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
	// ChangesBatchSize controls how many revisions may be batched per changes message.
	ChangesBatchSize uint16
	// CheckpointInterval triggers a checkpoint to be set this often.
	CheckpointInterval time.Duration
	// CheckpointRevCount controls how many revs to store before attempting to save a checkpoint.
	Direction ActiveReplicatorDirection
	// Continuous specifies whether the replication should be continuous or one-shot.
	Continuous bool
	// RemoteDBURL represents the full Sync Gateway URL, including database path, and basic auth credentials of the target.
	RemoteDBURL *url.URL
	// PurgeOnRemoval will purge the document on the active side if we pull a removal from the remote.
	PurgeOnRemoval bool
	// ActiveDB is a reference to the active database context.
	ActiveDB *Database
	// WebsocketPingInterval is the time between websocket heartbeats sent by the active replicator.
	WebsocketPingInterval time.Duration
	// Conflict resolver
	ConflictResolverFunc ConflictResolverFunc
	// SGR1CheckpointID can be used as a fallback when SGR2 checkpoints can't be found.
	SGR1CheckpointID string
	// InitialReconnectInterval is the initial time to wait for exponential backoff reconnects.
	InitialReconnectInterval time.Duration
	// MaxReconnectInterval is the maximum amount of time to wait between exponential backoff reconnect attempts.
	MaxReconnectInterval time.Duration
	// TotalReconnectTimeout, if non-zero, is the amount of time to wait before giving up trying to reconnect.
	TotalReconnectTimeout time.Duration

	// Delta sync enabled
	DeltasEnabled bool

	// InsecureSkipVerify determines whether the TLS certificate verification should be
	// disabled during replication. TLS certificate verification is enabled by default.
	InsecureSkipVerify bool

	// Callback to be invoked on replication completion
	onComplete OnCompleteFunc

	// Map corresponding to db.replications.[replicationID] in Sync Gateway's expvars.  Populated with
	// replication stats in blip_sync_stats.go
	ReplicationStatsMap *base.DbReplicatorStats
}

type OnCompleteFunc func(replicationID string)

// CheckpointHash returns a deterministic hash of the given config to be used as a checkpoint ID.
// TODO: Might be a way of caching this value? But need to be sure no config values wil change without clearing the cached hash.
func (arc ActiveReplicatorConfig) CheckpointHash() (string, error) {
	hash := sha1.New()

	// For each field in the config that affects replication result, append its value to the hasher.

	// Probably a neater way of doing this using struct tags and a type switch,
	// but the ActiveReplicatorConfig might end up being replaced with the existing replicator config.

	if _, err := hash.Write([]byte(arc.ID)); err != nil {
		return "", err
	}

	if _, err := hash.Write([]byte(arc.Filter)); err != nil {
		return "", err
	}
	if _, err := hash.Write([]byte(strings.Join(arc.FilterChannels, ","))); err != nil {
		return "", err
	}
	if _, err := hash.Write([]byte(strings.Join(arc.DocIDs, ","))); err != nil {
		return "", err
	}
	if _, err := hash.Write([]byte(strconv.FormatBool(arc.ActiveOnly))); err != nil {
		return "", err
	}
	if _, err := hash.Write([]byte(arc.Direction)); err != nil {
		return "", err
	}
	if _, err := hash.Write([]byte(arc.RemoteDBURL.String())); err != nil {
		return "", err
	}
	bucketUUID, err := arc.ActiveDB.Bucket.UUID()
	if err != nil {
		return "", err
	}
	if _, err := hash.Write([]byte(bucketUUID)); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
