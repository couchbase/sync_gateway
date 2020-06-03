package replicator

import (
	"fmt"

	"github.com/couchbase/go-blip"
)

// ActivePullReplicator is a unidirectional pull active replicator.
type ActivePullReplicator struct {
	config          *ActiveReplicatorConfig
	blipSyncContext *BlipSyncContext
	blipSender      *blip.Sender
}

func NewPullReplicator(config *ActiveReplicatorConfig) *ActivePullReplicator {
	return &ActivePullReplicator{
		config: config,
	}
}

func (apr *ActivePullReplicator) connect() error {
	if apr == nil {
		return fmt.Errorf("nil ActivePullReplicator, can't connect")
	}

	if apr.blipSender != nil {
		return fmt.Errorf("replicator already has a blipSender, can't connect twice")
	}

	blipContext := blip.NewContextCustomID(apr.config.ID+"-pull", blipCBMobileReplication)
	bsc := NewBlipSyncContext(blipContext, apr.config.ActiveDB, blipContext.ID)
	apr.blipSyncContext = bsc

	s, err := blipSync(*apr.config.PassiveDB, apr.blipSyncContext.blipContext)
	if err != nil {
		return err
	}
	apr.blipSender = s

	return nil
}

func (apr *ActivePullReplicator) Close() error {
	if apr == nil {
		// noop
		return nil
	}

	if apr.blipSender != nil {
		apr.blipSender.Close()
		apr.blipSender = nil
	}

	if apr.blipSyncContext != nil {
		apr.blipSyncContext.Close()
		apr.blipSyncContext = nil
	}

	return nil
}

func (apr *ActivePullReplicator) Start() error {
	if apr == nil {
		return fmt.Errorf("nil ActivePullReplicator, can't start")
	}

	if err := apr.connect(); err != nil {
		return err
	}

	subChangesRequest := SubChangesRequest{
		Continuous:     apr.config.Continuous,
		Batch:          apr.config.ChangesBatchSize,
		Since:          apr.config.Since,
		Filter:         apr.config.Filter,
		FilterChannels: apr.config.FilterChannels,
		DocIDs:         apr.config.DocIDs,
		ActiveOnly:     apr.config.ActiveOnly,
	}

	if err := subChangesRequest.Send(apr.blipSender); err != nil {
		return err
	}

	return nil
}
