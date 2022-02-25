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
	"errors"
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/robertkrimen/otto"
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

func NewConflictResolverFunc(resolverType ConflictResolverType, customResolverSource string) (ConflictResolverFunc, error) {
	switch resolverType {
	case ConflictResolverLocalWins:
		return LocalWinsConflictResolver, nil
	case ConflictResolverRemoteWins:
		return RemoteWinsConflictResolver, nil
	case ConflictResolverDefault:
		return DefaultConflictResolver, nil
	case ConflictResolverCustom:
		return NewCustomConflictResolver(customResolverSource)
	default:
		return nil, fmt.Errorf("Unknown Conflict Resolver type: %s", resolverType)
	}
}

// NewCustomConflictResolver returns a ConflictResolverFunc that executes the
// javascript conflict resolver specified by source
func NewCustomConflictResolver(source string) (ConflictResolverFunc, error) {
	conflictResolverJSServer := NewConflictResolverJSServer(source)
	return conflictResolverJSServer.EvaluateFunction, nil
}

// ConflictResolverJSServer manages the compiled javascript function runner instance
type ConflictResolverJSServer struct {
	*sgbucket.JSServer
}

func NewConflictResolverJSServer(fnSource string) *ConflictResolverJSServer {
	base.DebugfCtx(context.Background(), base.KeyReplicate, "Creating new ConflictResolverFunction")
	return &ConflictResolverJSServer{
		JSServer: sgbucket.NewJSServer(fnSource, kTaskCacheSize, newConflictResolverRunner),
	}
}

// EvaluateFunction executes the conflict resolver with the provided conflict and returns the result.
func (i *ConflictResolverJSServer) EvaluateFunction(conflict Conflict) (Body, error) {
	docID, _ := conflict.LocalDocument[BodyId].(string)
	localRevID, _ := conflict.LocalDocument[BodyRev].(string)
	remoteRevID, _ := conflict.RemoteDocument[BodyRev].(string)
	result, err := i.Call(conflict)
	if err != nil {
		base.WarnfCtx(context.Background(), "Unexpected error invoking conflict resolver for document %s, local/remote revisions %s/%s - processing aborted, document will not be replicated.  Error: %v",
			base.UD(docID), base.UD(localRevID), base.UD(remoteRevID), err)
		return nil, err
	}

	// A null value returned by the conflict resolver should be treated as a delete
	if result == nil {
		return Body{BodyDeleted: true}, nil
	}

	switch result := result.(type) {
	case Body:
		return result, nil
	case map[string]interface{}:
		return result, nil
	case error:
		base.WarnfCtx(context.Background(), "conflictResolverRunner: "+result.Error())
		return nil, result
	default:
		base.WarnfCtx(context.Background(), "Custom conflict resolution function returned non-document result %v Type: %T", result, result)
		return nil, errors.New("Custom conflict resolution function returned non-document value.")
	}
}

// Compiles a JavaScript event function to a conflictResolverRunner object.
func newConflictResolverRunner(funcSource string) (sgbucket.JSServerTask, error) {
	conflictResolverRunner := &sgbucket.JSRunner{}
	err := conflictResolverRunner.InitWithLogging(funcSource,
		func(s string) {
			base.ErrorfCtx(context.Background(), base.KeyJavascript.String()+": ConflictResolver %s", base.UD(s))
		},
		func(s string) {
			base.InfofCtx(context.Background(), base.KeyJavascript, "ConflictResolver %s", base.UD(s))
		})
	if err != nil {
		return nil, err
	}

	// Implementation of the 'defaultPolicy(conflict)' callback:
	conflictResolverRunner.DefineNativeFunction("defaultPolicy", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) == 0 {
			return ErrorToOttoValue(conflictResolverRunner, errors.New("No conflict parameter specified when calling defaultPolicy()"))
		}
		rawConflict, exportErr := call.Argument(0).Export()
		if exportErr != nil {
			return ErrorToOttoValue(conflictResolverRunner, fmt.Errorf("Unable to export conflict parameter for defaultPolicy(): %v Error: %s", call.Argument(0), exportErr))
		}

		// Called defaultPolicy with null/undefined value - return
		if rawConflict == nil || call.Argument(0).IsUndefined() {
			return ErrorToOttoValue(conflictResolverRunner, errors.New("Null or undefined value passed to defaultPolicy()"))
		}

		conflict, ok := rawConflict.(Conflict)
		if !ok {
			return ErrorToOttoValue(conflictResolverRunner, fmt.Errorf("Invalid value passed to defaultPolicy().  Value was type %T, expected type Conflict", rawConflict))
		}

		defaultWinner, _ := DefaultConflictResolver(conflict)
		ottoDefaultWinner, err := conflictResolverRunner.ToValue(defaultWinner)
		if err != nil {
			return ErrorToOttoValue(conflictResolverRunner, fmt.Errorf("Error converting default winner to javascript value.  Error:%w", err))
		}
		return ottoDefaultWinner
	})

	conflictResolverRunner.After = func(result otto.Value, err error) (interface{}, error) {
		nativeValue, _ := result.Export()
		return nativeValue, err
	}

	return conflictResolverRunner, nil
}

// Converts an error to an otto value, to support native functions returning errors.
func ErrorToOttoValue(runner *sgbucket.JSRunner, err error) otto.Value {
	errorValue, convertErr := runner.ToValue(err)
	if convertErr != nil {
		base.WarnfCtx(context.Background(), "Unable to convert error to otto value: %v", convertErr)
	}
	return errorValue
}
