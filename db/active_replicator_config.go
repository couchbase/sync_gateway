package db

import (
	"crypto/sha1"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type ActiveReplicatorDirection string

const (
	ActiveReplicatorTypePushAndPull ActiveReplicatorDirection = "pushAndPull"
	ActiveReplicatorTypePush                                  = "push"
	ActiveReplicatorTypePull                                  = "pull"
)

func (d ActiveReplicatorDirection) IsValid() bool {
	switch d {
	case ActiveReplicatorTypePushAndPull, ActiveReplicatorTypePush, ActiveReplicatorTypePull:
		return true
	default:
		return false
	}
}

const (
	defaultCheckpointInterval = time.Second * 30
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
	// ChangesBatchSize controls how many revisions may be batched per changes message.
	ChangesBatchSize uint16
	// CheckpointInterval triggers a checkpoint to be set this often.
	CheckpointInterval time.Duration
	// CheckpointRevCount controls how many revs to store before attempting to save a checkpoint.
	Direction ActiveReplicatorDirection
	// Continuous specifies whether the replication should be continuous or one-shot.
	Continuous bool
	// PassiveDBURL represents the full Sync Gateway URL, including database path, and basic auth credentials of the target.
	PassiveDBURL *url.URL

	// ActiveDB is a reference to the active database context.
	ActiveDB *Database
}

// CheckpointHash returns a deterministic hash of the given config to be used as a checkpoint ID.
// TODO: Might be a way of caching this value? But need to be sure no config values wil change without clearing the cached hash.
func (arc ActiveReplicatorConfig) CheckpointHash() (string, error) {
	hash := sha1.New()

	// For each field in the config that affects replication result, append its value to the hasher.

	// Probably a neater way of doing this using struct tags and a type switch,
	// but the ActiveReplicatorConfig might end up being replaced with the existing replicator config.
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
	if _, err := hash.Write([]byte(arc.PassiveDBURL.String())); err != nil {
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
