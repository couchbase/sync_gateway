/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"crypto/sha1"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
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
	// FilterChannels are a set of channels to be used by the sync_gateway/bychannel filter. Access via getFilteredChannels.
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
	// RunAs is the user to run the replication under
	RunAs string
	// PurgeOnRemoval will purge the document on the active side if we pull a removal from the remote.
	PurgeOnRemoval bool
	// ActiveDB is a reference to the active database context.
	ActiveDB *Database
	// WebsocketPingInterval is the time between websocket heartbeats sent by the active replicator.
	WebsocketPingInterval time.Duration
	// Conflict resolver
	ConflictResolverFunc ConflictResolverFunc
	// Conflict resolution type.  Required for Equals comparison only
	ConflictResolutionType ConflictResolverType
	// Conflict resolver source, for custom conflict resolver.  Required for Equals comparison only
	ConflictResolverFuncSrc string
	// InitialReconnectInterval is the initial time to wait for exponential backoff reconnects.
	InitialReconnectInterval time.Duration
	// MaxReconnectInterval is the maximum amount of time to wait between exponential backoff reconnect attempts.
	MaxReconnectInterval time.Duration
	// TotalReconnectTimeout, if non-zero, is the amount of time to wait before giving up trying to reconnect. Zero disables reconnect entirely.
	TotalReconnectTimeout time.Duration

	// CollectionsEnabled can be set to replicate one or more named collections, rather than just the default collection.
	CollectionsEnabled bool
	// CollectionsLocal represents a list of dot-separated scope/collections that will be replicated.
	// This slice can be empty to replicate all scopes/collections for the database when CollectionsEnabled is set to true.
	CollectionsLocal []string // list of local/active "scope.collection"
	// CollectionsRemote represents an equivalent list of dot-separated scope/collections that the local collections will be remapped to on the remote/passive side.
	// This slice can be empty to replicate all scopes/collections for the database when CollectionsEnabled is set to true.
	CollectionsRemote []string // list of remote/passive "scope.collection"
	// CollectionsChannelFilter represents a list of channels to be replicated for each collection. Access via getFilteredChannels.
	CollectionsChannelFilter [][]string // list of channels to replicate for each collection.

	// Delta sync enabled
	DeltasEnabled bool

	// InsecureSkipVerify determines whether the TLS certificate verification should be
	// disabled during replication. TLS certificate verification is enabled by default.
	InsecureSkipVerify bool

	// Callback to be invoked on replication completion
	onComplete OnCompleteFunc

	// checkpointPrefix is the prefix for checkpoint ID on activeReplicatorCommon which is used for replication checkpoints
	checkpointPrefix string

	// Map corresponding to db.replications.[replicationID] in Sync Gateway's expvars.  Populated with
	// replication stats in blip_sync_stats.go
	ReplicationStatsMap *base.DbReplicatorStats

	// Returns an error from ActiveReplicator.Stop() if the replicator encountered any recovered panics inside handlers.
	// Intended for test usage only, enabled by default but disabled for the prod-ISGR codepath.
	reportHandlerPanicsOnStop *bool
}

// SetCheckpointPrefix is a cross-package way of defining a checkpoint prefix for an ActiveReplicatorConfig intended for test usage.
func (arc *ActiveReplicatorConfig) SetCheckpointPrefix(_ testing.TB, s string) {
	arc.checkpointPrefix = s
}

type OnCompleteFunc func(replicationID string)

// CheckpointHash returns a deterministic hash of the given config to be used as part of a checkpoint's validity.
// TODO: Might be a way of caching this value? But need to be sure no config values will change without clearing the cached hash.
func (arc ActiveReplicatorConfig) CheckpointHash(collectionIdx *int) (string, error) {
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

	if _, err := hash.Write([]byte(strings.Join(arc.getFilteredChannels(collectionIdx), ","))); err != nil {
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
	if _, err := hash.Write([]byte(arc.RunAs)); err != nil {
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

// Equals returns true if the given config is equal to the receiver. Used to detect when a replication config has changed via isCfgChanged
func (arc *ActiveReplicatorConfig) Equals(other *ActiveReplicatorConfig) bool {

	if arc.ID != other.ID {
		return false
	}

	if arc.Filter != other.Filter {
		return false
	}

	if !reflect.DeepEqual(arc.FilterChannels, other.FilterChannels) {
		return false
	}

	if !reflect.DeepEqual(arc.DocIDs, other.DocIDs) {
		return false
	}

	if arc.ActiveOnly != other.ActiveOnly {
		return false
	}

	if arc.ChangesBatchSize != other.ChangesBatchSize {
		return false
	}

	if arc.CheckpointInterval != other.CheckpointInterval {
		return false
	}

	if arc.Direction != other.Direction {
		return false
	}

	if arc.Continuous != other.Continuous {
		return false
	}

	if !reflect.DeepEqual(arc.RemoteDBURL, other.RemoteDBURL) {
		return false
	}

	if arc.PurgeOnRemoval != other.PurgeOnRemoval {
		return false
	}

	if arc.WebsocketPingInterval != other.WebsocketPingInterval {
		return false
	}

	if arc.ConflictResolutionType != other.ConflictResolutionType {
		return false
	}

	if arc.ConflictResolverFuncSrc != other.ConflictResolverFuncSrc {
		return false
	}

	if arc.InitialReconnectInterval != other.InitialReconnectInterval {
		return false
	}

	if arc.MaxReconnectInterval != other.MaxReconnectInterval {
		return false
	}

	if arc.TotalReconnectTimeout != other.TotalReconnectTimeout {
		return false
	}

	if arc.DeltasEnabled != other.DeltasEnabled {
		return false
	}

	if arc.CollectionsEnabled != other.CollectionsEnabled {
		return false
	}

	if !reflect.DeepEqual(arc.CollectionsLocal, other.CollectionsLocal) {
		return false
	}

	if !reflect.DeepEqual(arc.CollectionsRemote, other.CollectionsRemote) {
		return false
	}

	if !reflect.DeepEqual(arc.CollectionsChannelFilter, other.CollectionsChannelFilter) {
		return false
	}

	return true
}
