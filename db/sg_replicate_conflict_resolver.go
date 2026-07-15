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
	"time"

	"github.com/dop251/goja"

	sgbucket "github.com/couchbase/sg-bucket"
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
// the same approach used for doc and oldDoc in the Sync Function. LocalHLV and RemoteHLV
// will be the documents HLVs.
type Conflict struct {
	LocalDocument  Body `json:"LocalDocument"`
	RemoteDocument Body `json:"RemoteDocument"`
	LocalHLV       *HybridLogicalVector
	RemoteHLV      *HybridLogicalVector
}

// Definition of the ConflictResolverFunc API.  Winner may be one of
// conflict.LocalDocument or conflict.RemoteDocument, or a new Body
// based on a merge of the two.
//   - In the merge case, winner[revid] must be empty.
//   - If an nil Body is returned, the conflict should be resolved as a deletion/tombstone.
type ConflictResolverFunc func(ctx context.Context, conflict Conflict) (winner Body, err error)

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

// ConflictResolvers is a container for both revTree and HLV conflict resolvers
type ConflictResolvers struct {
	revTreeConflictResolver *ConflictResolver
	hlvConflictResolver     *ConflictResolver
}

func (c *ConflictResolvers) IsEmpty() bool {
	return c == nil || (c.revTreeConflictResolver == nil && c.hlvConflictResolver == nil)
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
func (c *ConflictResolver) Resolve(ctx context.Context, conflict Conflict) (winner Body, resolutionType ConflictResolutionType, err error) {

	winner, err = c.crf(ctx, conflict)
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

	base.InfofCtx(ctx, base.KeyReplicate, "Conflict resolver returned non-empty revID (%s) not matching local (%s) or remote (%s), treating result as merge.", winningRev, localRev, remoteRev)
	c.stats.ConflictResultMergeCount.Add(1)
	return winner, ConflictResolutionMerge, err
}

// ResolveForHLV is a wrapper for ConflictResolverFunc that evaluates whether conflict resolution resulted in localWins,
// remoteWins, or merge, specifically for HLV-aware conflict resolution.
func (c *ConflictResolver) ResolveForHLV(ctx context.Context, conflict Conflict) (winner Body, resolutionType ConflictResolutionType, err error) {

	winner, err = c.crf(ctx, conflict)
	if err != nil {
		return winner, "", err
	}

	winningRev, ok := winner[BodyCV]
	if !ok {
		c.stats.ConflictResultMergeCount.Add(1)
		return winner, ConflictResolutionMerge, nil
	}

	localRev, ok := conflict.LocalDocument[BodyCV]
	if ok && localRev == winningRev {
		c.stats.ConflictResultLocalCount.Add(1)
		return winner, ConflictResolutionLocal, nil
	}

	remoteRev, ok := conflict.RemoteDocument[BodyCV]
	if ok && remoteRev == winningRev {
		c.stats.ConflictResultRemoteCount.Add(1)
		return winner, ConflictResolutionRemote, nil
	}

	base.WarnfCtx(ctx, "Conflict resolver returned non-empty cv (%s) not matching local (%s) or remote (%s).", winningRev, localRev, remoteRev)
	return winner, "", errors.New("conflict resolver returned non-empty cv not matching local or remote")
}

// DefaultConflictResolver uses the same logic as revTree.WinningRevision,
// with the exception that a deleted revision is picked as the winner:
// the revision whose (deleted, generation, hash) tuple compares the highest.
// Returns error to satisfy ConflictResolverFunc signature.
func DefaultConflictResolver(ctx context.Context, conflict Conflict) (result Body, err error) {
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
	if compareRevIDs(ctx, localRevID, remoteRevID) >= 0 {
		return conflict.LocalDocument, nil
	} else {
		return conflict.RemoteDocument, nil
	}
}

// LocalWinsConflictResolver returns the local document as winner
func LocalWinsConflictResolver(_ context.Context, conflict Conflict) (winner Body, err error) {
	return conflict.LocalDocument, nil
}

// RemoteWinsConflictResolver returns the local document as-is
func RemoteWinsConflictResolver(_ context.Context, conflict Conflict) (winner Body, err error) {
	return conflict.RemoteDocument, nil
}

func NewConflictResolverFunc(ctx context.Context, resolverType ConflictResolverType, customResolverSource string, customResolverTimeout time.Duration) (ConflictResolverFunc, error) {
	switch resolverType {
	case ConflictResolverLocalWins:
		return LocalWinsConflictResolver, nil
	case ConflictResolverRemoteWins:
		return RemoteWinsConflictResolver, nil
	case ConflictResolverDefault:
		return DefaultConflictResolver, nil
	case ConflictResolverCustom:
		return NewCustomConflictResolver(ctx, customResolverSource, customResolverTimeout)
	default:
		return nil, fmt.Errorf("Unknown Conflict Resolver type: %s", resolverType)
	}
}

func NewConflictResolverFuncForHLV(ctx context.Context, resolverType ConflictResolverType, customResolverSource string, customResolverTimeout time.Duration) (ConflictResolverFunc, error) {
	switch resolverType {
	case ConflictResolverLocalWins:
		return LocalWinsConflictResolver, nil
	case ConflictResolverRemoteWins:
		return RemoteWinsConflictResolver, nil
	case ConflictResolverDefault:
		return DefaultLWWConflictResolutionType, nil
	case ConflictResolverCustom:
		return NewCustomConflictResolver(ctx, customResolverSource, customResolverTimeout)
	default:
		return nil, fmt.Errorf("Unknown Conflict Resolver type: %s", resolverType)
	}
}

// NewCustomConflictResolver returns a ConflictResolverFunc that executes the
// javascript conflict resolver specified by source
func NewCustomConflictResolver(ctx context.Context, source string, timeout time.Duration) (ConflictResolverFunc, error) {
	conflictResolverJSServer := NewConflictResolverJSServer(ctx, source, timeout)
	return conflictResolverJSServer.EvaluateFunction, nil
}

// ConflictResolverJSServer manages the compiled javascript function runner instance
type ConflictResolverJSServer struct {
	*sgbucket.JSServer
}

func NewConflictResolverJSServer(ctx context.Context, fnSource string, timeout time.Duration) *ConflictResolverJSServer {
	return &ConflictResolverJSServer{
		JSServer: sgbucket.NewJSServer(ctx, fnSource, timeout, kTaskCacheSize, newConflictResolverRunner),
	}
}

// EvaluateFunction executes the conflict resolver with the provided conflict and returns the result.
func (i *ConflictResolverJSServer) EvaluateFunction(ctx context.Context, conflict Conflict) (Body, error) {
	docID, _ := conflict.LocalDocument[BodyId].(string)
	localRevID, _ := conflict.LocalDocument[BodyRev].(string)
	remoteRevID, _ := conflict.RemoteDocument[BodyRev].(string)
	result, err := i.Call(ctx, conflictToJSInput(conflict))
	if err != nil {
		base.WarnfCtx(ctx, "Unexpected error invoking conflict resolver for document %s, local/remote revisions %s/%s - processing aborted, document will not be replicated.  Error: %v",
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
	case map[string]any:
		return result, nil
	case error:
		base.WarnfCtx(ctx, "conflictResolverRunner: %s", result.Error())
		return nil, result
	default:
		base.WarnfCtx(ctx, "Custom conflict resolution function returned non-document result %v Type: %T", result, result)
		return nil, errors.New("Custom conflict resolution function returned non-document value.")
	}
}

// Compiles a JavaScript event function to a conflictResolverRunner object.
func newConflictResolverRunner(ctx context.Context, funcSource string, timeout time.Duration) (sgbucket.JSServerTask, error) {
	conflictResolverRunner := &sgbucket.JSRunner{}
	err := conflictResolverRunner.InitWithLogging(funcSource, timeout,
		func(s string) {
			base.ErrorfCtx(ctx, base.KeyJavascript.String()+": ConflictResolver %s", base.UD(s))
		},
		func(s string) {
			base.InfofCtx(ctx, base.KeyJavascript, "ConflictResolver %s", base.UD(s))
		})
	if err != nil {
		return nil, err
	}

	// Implementation of the 'defaultPolicy(conflict)' callback:
	conflictResolverRunner.DefineNativeFunction("defaultPolicy", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) == 0 {
			return ErrorToJSValue(conflictResolverRunner, errors.New("No conflict parameter specified when calling defaultPolicy()"))
		}
		arg0 := call.Argument(0)
		rawConflict := sgbucket.ExportValue(arg0)

		// Called defaultPolicy with null/undefined value - return
		if rawConflict == nil || goja.IsUndefined(arg0) {
			return ErrorToJSValue(conflictResolverRunner, errors.New("Null or undefined value passed to defaultPolicy()"))
		}

		conflict, ok := conflictFromJSValue(rawConflict)
		if !ok {
			return ErrorToJSValue(conflictResolverRunner, fmt.Errorf("Invalid value passed to defaultPolicy().  Value was type %T, expected type Conflict", rawConflict))
		}

		defaultWinner, _ := DefaultConflictResolver(ctx, conflict)
		return conflictResolverRunner.ToValue(defaultWinner)
	})

	conflictResolverRunner.After = func(result goja.Value, err error) (any, error) {
		return sgbucket.ExportValue(result), err
	}

	return conflictResolverRunner, nil
}

// Converts an error to a JS value, to support native functions returning errors.
func ErrorToJSValue(runner *sgbucket.JSRunner, err error) goja.Value {
	return runner.ToValue(err)
}

// conflictToJSInput converts a Conflict into the value passed as the resolver function's
// `conflict` argument. LocalDocument/RemoteDocument are re-typed from Body to the unnamed,
// method-less map[string]interface{} (a cheap reinterpretation, not a copy): goja only gives
// a Go map property-style access to its entries (e.g. `conflict.LocalDocument.someProp`) when
// it's that literal type with no methods; Body's methods would otherwise make goja expose it
// as an opaque Go object exposing only those methods.
func conflictToJSInput(conflict Conflict) map[string]any {
	return map[string]any{
		"LocalDocument":  map[string]any(conflict.LocalDocument),
		"RemoteDocument": map[string]any(conflict.RemoteDocument),
		"LocalHLV":       conflict.LocalHLV,
		"RemoteHLV":      conflict.RemoteHLV,
	}
}

// conflictFromJSValue reconstructs a Conflict from an exported JS value shaped like the map
// conflictToJSInput produces (the inverse conversion), for the `defaultPolicy` native callback.
func conflictFromJSValue(raw any) (Conflict, bool) {
	m, ok := raw.(map[string]any)
	if !ok {
		return Conflict{}, false
	}
	localDoc, ok1 := m["LocalDocument"].(map[string]any)
	remoteDoc, ok2 := m["RemoteDocument"].(map[string]any)
	if !ok1 || !ok2 {
		return Conflict{}, false
	}
	conflict := Conflict{LocalDocument: Body(localDoc), RemoteDocument: Body(remoteDoc)}
	if hlv, ok := m["LocalHLV"].(*HybridLogicalVector); ok {
		conflict.LocalHLV = hlv
	}
	if hlv, ok := m["RemoteHLV"].(*HybridLogicalVector); ok {
		conflict.RemoteHLV = hlv
	}
	return conflict, true
}
