package db

import (
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

// DefaultConflictResolver uses the same logic as revTree.WinningRevision:
// the revision whose (!deleted, generation, hash) tuple compares the highest.
func DefaultConflictResolver(conflict Conflict) (result Body, err error) {
	localDeleted, _ := conflict.LocalDocument[BodyDeleted].(bool)
	remoteDeleted, _ := conflict.RemoteDocument[BodyDeleted].(bool)
	if localDeleted && !remoteDeleted {
		return conflict.RemoteDocument, nil
	}

	if remoteDeleted && !localDeleted {
		return conflict.LocalDocument, nil
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
	base.Debugf(base.KeyReplicate, "Creating new ConflictResolverFunction")
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
		base.Warnf("Unexpected error invoking conflict resolver for document %s, local/remote revisions %s/%s - processing aborted, document will not be replicated.  Error: %v",
			base.UD(docID), base.UD(localRevID), base.UD(remoteRevID), err)
		return nil, err
	}
	switch result := result.(type) {
	case Body:
		return result, nil
	case map[string]interface{}:
		return result, nil
	default:
		base.Warnf("Custom conflict resolution function returned non-document result %v Type: %T", result, result)
		return nil, errors.New("Custom conflict resolution function returned non-document value.")
	}
}

// Compiles a JavaScript event function to a conflictResolverRunner object.
func newConflictResolverRunner(funcSource string) (sgbucket.JSServerTask, error) {
	conflictResolverRunner := &sgbucket.JSRunner{}
	err := conflictResolverRunner.InitWithLogging(funcSource,
		func(s string) { base.Errorf(base.KeyJavascript.String()+": ConflictResolver %s", base.UD(s)) },
		func(s string) { base.Infof(base.KeyJavascript, "ConflictResolver %s", base.UD(s)) })
	if err != nil {
		return nil, err
	}

	conflictResolverRunner.After = func(result otto.Value, err error) (interface{}, error) {
		nativeValue, _ := result.Export()
		return nativeValue, err
	}

	return conflictResolverRunner, nil
}
