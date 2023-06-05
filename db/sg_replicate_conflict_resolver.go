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
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/sg-bucket/js"
	"github.com/couchbase/sync_gateway/base"
)

type ConflictResolverType string

const (
	ConflictResolverLocalWins  ConflictResolverType = "localWins"
	ConflictResolverRemoteWins ConflictResolverType = "remoteWins"
	ConflictResolverDefault    ConflictResolverType = "default"
	ConflictResolverCustom     ConflictResolverType = "custom"
)

func (d ConflictResolverType) IsValid() bool {
	switch d {
	case ConflictResolverLocalWins, ConflictResolverRemoteWins, ConflictResolverDefault, ConflictResolverCustom:
		return true
	default:
		return false
	}
}

// ConflictResolutionType is used to identify the Body returned by a conflict resolution function
// as local, remote, or merge
type ConflictResolutionType string

const (
	ConflictResolutionLocal  ConflictResolutionType = "local"
	ConflictResolutionRemote ConflictResolutionType = "remote"
	ConflictResolutionMerge  ConflictResolutionType = "merge"
)

// Conflict is the input to all conflict resolvers.  LocalDocument and RemoteDocument
// are expected to be document bodies with metadata injected into the body following
// the same approach used for doc and oldDoc in the Sync Function
type Conflict struct {
	LocalDocument  Body `json:"LocalDocument"`
	RemoteDocument Body `json:"RemoteDocument"`
}

// Definition of the ConflictResolverFunc API.  Winner may be one of
// conflict.LocalDocument or conflict.RemoteDocument, or a new Body
// based on a merge of the two.
//   - In the merge case, winner[revid] must be empty.
//   - If an nil Body is returned, the conflict should be resolved as a deletion/tombstone.
type ConflictResolverFunc func(conflict Conflict) (winner Body, err error)

type ConflictResolverStats struct {
	ConflictResultMergeCount  *base.SgwIntStat
	ConflictResultLocalCount  *base.SgwIntStat
	ConflictResultRemoteCount *base.SgwIntStat
}

func DefaultConflictResolverStats() *ConflictResolverStats {
	return &ConflictResolverStats{
		ConflictResultMergeCount:  &base.SgwIntStat{},
		ConflictResultLocalCount:  &base.SgwIntStat{},
		ConflictResultRemoteCount: &base.SgwIntStat{},
	}
}

// NewConflictResolverStats initializes the replications stats inside the provided container, and returns
// a ConflictResolverStats to manage interaction with those stats.  If the container is not specified, expvar stats
// will not be published.
func NewConflictResolverStats(container *base.DbReplicatorStats) *ConflictResolverStats {
	if container == nil {
		return DefaultConflictResolverStats()
	}
	return &ConflictResolverStats{
		ConflictResultMergeCount:  container.ConflictResolvedMergedCount,
		ConflictResultLocalCount:  container.ConflictResolvedLocalCount,
		ConflictResultRemoteCount: container.ConflictResolvedRemoteCount,
	}
}

type ConflictResolver struct {
	crf   ConflictResolverFunc
	stats *ConflictResolverStats
}

func NewConflictResolver(crf ConflictResolverFunc, statsContainer *base.DbReplicatorStats) *ConflictResolver {
	resolver := &ConflictResolver{
		crf:   crf,
		stats: NewConflictResolverStats(statsContainer),
	}
	return resolver
}

// Wrapper for ConflictResolverFunc that evaluates whether conflict resolution resulted in
// localWins, remoteWins, or merge
func (c *ConflictResolver) Resolve(conflict Conflict) (winner Body, resolutionType ConflictResolutionType, err error) {

	winner, err = c.crf(conflict)
	if err != nil {
		return winner, "", err
	}

	winningRev, ok := winner[BodyRev]
	if !ok {
		c.stats.ConflictResultMergeCount.Add(1)
		return winner, ConflictResolutionMerge, nil
	}

	localRev, ok := conflict.LocalDocument[BodyRev]
	if ok && localRev == winningRev {
		c.stats.ConflictResultLocalCount.Add(1)
		return winner, ConflictResolutionLocal, nil
	}

	remoteRev, ok := conflict.RemoteDocument[BodyRev]
	if ok && remoteRev == winningRev {
		c.stats.ConflictResultRemoteCount.Add(1)
		return winner, ConflictResolutionRemote, nil
	}

	base.InfofCtx(context.Background(), base.KeyReplicate, "Conflict resolver returned non-empty revID (%s) not matching local (%s) or remote (%s), treating result as merge.", winningRev, localRev, remoteRev)
	c.stats.ConflictResultMergeCount.Add(1)
	return winner, ConflictResolutionMerge, err
}

// DefaultConflictResolver uses the same logic as revTree.WinningRevision,
// with the exception that a deleted revision is picked as the winner:
// the revision whose (deleted, generation, hash) tuple compares the highest.
// Returns error to satisfy ConflictResolverFunc signature.
func DefaultConflictResolver(conflict Conflict) (result Body, err error) {
	localDeleted, _ := conflict.LocalDocument[BodyDeleted].(bool)
	remoteDeleted, _ := conflict.RemoteDocument[BodyDeleted].(bool)
	if localDeleted && !remoteDeleted {
		return conflict.LocalDocument, nil
	}
	if remoteDeleted && !localDeleted {
		return conflict.RemoteDocument, nil
	}

	localRevID, _ := conflict.LocalDocument[BodyRev].(string)
	remoteRevID, _ := conflict.RemoteDocument[BodyRev].(string)
	if compareRevIDs(localRevID, remoteRevID) >= 0 {
		return conflict.LocalDocument, nil
	} else {
		return conflict.RemoteDocument, nil
	}
}

// LocalWinsConflictResolver returns the local document as winner
func LocalWinsConflictResolver(conflict Conflict) (winner Body, err error) {
	return conflict.LocalDocument, nil
}

// RemoteWinsConflictResolver returns the local document as-is
func RemoteWinsConflictResolver(conflict Conflict) (winner Body, err error) {
	return conflict.RemoteDocument, nil
}

//////// CUSTOM CONFLICT RESOLVER:

func NewConflictResolverFunc(dbc *DatabaseContext, resolverType ConflictResolverType, customResolverSource string) (ConflictResolverFunc, error) {
	switch resolverType {
	case ConflictResolverLocalWins:
		return LocalWinsConflictResolver, nil
	case ConflictResolverRemoteWins:
		return RemoteWinsConflictResolver, nil
	case ConflictResolverDefault:
		return DefaultConflictResolver, nil
	case ConflictResolverCustom:
		return NewCustomConflictResolver(customResolverSource, dbc.Options.JavascriptTimeout, &dbc.JS)
	default:
		return nil, fmt.Errorf("unknown Conflict Resolver type: %s", resolverType)
	}
}

// NewCustomConflictResolver returns a ConflictResolverFunc that executes the
// javascript conflict resolver specified by source
func NewCustomConflictResolver(source string, timeout time.Duration, host js.ServiceHost) (ConflictResolverFunc, error) {
	base.DebugfCtx(context.Background(), base.KeyReplicate, "Creating new ConflictResolverFunction")
	service := js.NewService(host, "conflict resolver", fmt.Sprintf(kConflictResolverJSCode, source))

	return func(conflict Conflict) (Body, error) {
		// Here's the ConflictResolverFunc:
		ctx := context.Background()
		if timeout > 0 {
			var cancelFn context.CancelFunc
			ctx, cancelFn = context.WithTimeout(ctx, timeout)
			defer cancelFn()
		}

		result, err := service.Run(ctx, conflict)

		if err != nil {
			// Warn if the resolver fn fails:
			docID, _ := conflict.LocalDocument[BodyId].(string)
			localRevID, _ := conflict.LocalDocument[BodyRev].(string)
			remoteRevID, _ := conflict.RemoteDocument[BodyRev].(string)
			base.WarnfCtx(context.Background(), "Unexpected error invoking conflict resolver for document %s, local/remote revisions %s/%s - processing aborted, document will not be replicated.  Error: %+v",
				base.UD(docID), base.UD(localRevID), base.UD(remoteRevID), err)
			return nil, err
		}

		// A null value returned by the conflict resolver should be treated as a delete
		if result == nil {
			return Body{BodyDeleted: true}, nil
		}

		switch result := result.(type) {
		case map[string]interface{}:
			return result, nil
		case Body:
			return result, nil
		case error:
			base.WarnfCtx(context.Background(), "conflictResolverRunner: "+result.Error())
			return nil, result
		default:
			base.WarnfCtx(context.Background(), "Custom conflict resolution function returned non-document result %v Type: %T", result, result)
			return nil, errors.New("custom conflict resolution function returned non-document value")
		}
	}, nil
}

//go:embed sg_replicate_conflict_resolver.js
var kConflictResolverJSCode string
