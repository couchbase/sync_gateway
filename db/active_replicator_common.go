package db

import (
	"context"
	"expvar"
	"sync"

	"github.com/couchbase/go-blip"
)

// replicatorCommon defines the struct contents shared by ActivePushReplicator
// and ActivePullReplicator
type activeReplicatorCommon struct {
	config                  *ActiveReplicatorConfig
	blipSyncContext         *BlipSyncContext
	blipSender              *blip.Sender
	Stats                   expvar.Map
	Checkpointer            *Checkpointer
	checkpointerCtx         context.Context
	checkpointerCtxCancel   context.CancelFunc
	state                   string
	lastError               error
	replicationStats        *BlipSyncStats
	onReplicatorComplete    ReplicatorCompleteFunc
	replicatorCompleteMutex sync.Mutex
}

type ReplicatorCompleteFunc func()

// setErrorState updates state and lastError, and
// returns the error provided
func (a *activeReplicatorCommon) setError(err error) (passThrough error) {
	a.state = ReplicationStateError
	a.lastError = err
	return err
}

// setState updates replicator state and resets lastError to nil
func (a *activeReplicatorCommon) setState(state string) {
	a.state = state
	a.lastError = nil
}

func (a *activeReplicatorCommon) Reset() error {
	// TODO: pending CBG-908
	//  Since we require that a replication be stopped
	//  prior to reset, it's expected that checkpointer
	//  is unavailable.  This function will need to
	//  remove local checkpoints for push and pull replications
	//  using the config.ActiveDB
	return nil
}
