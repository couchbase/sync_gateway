package replicator

import (
	"fmt"

	"github.com/couchbase/go-blip"
)

// ActivePullReplicator is a unidirectional pull active replicator.
type ActivePullReplicator struct {
	config      *ActiveReplicatorConfig
	blipContext *blip.Context
	blipSender  *blip.Sender
}

// Compile-time interface check.
var _ ActiveReplicator = &ActivePullReplicator{}

func NewPullReplicator(config *ActiveReplicatorConfig) *ActivePullReplicator {
	return &ActivePullReplicator{
		config:      config,
		blipContext: blip.NewContextCustomID(config.ID+"-pull", blipCBMobileReplication),
	}
}

// Connects to the target via BLIP and sets.
func (apr *ActivePullReplicator) Connect() error {
	if apr == nil {
		// noop
		return nil
	}

	if apr.blipSender != nil {
		return fmt.Errorf("replicator already has a blipSender ... can't connect twice")
	}

	s, err := blipSync(*apr.config.TargetDB, apr.blipContext)
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

	return nil
}

func (apr *ActivePullReplicator) Start() error {
	if apr == nil {
		// noop
		return nil
	}

	if apr.blipSender == nil {
		return fmt.Errorf("ActiveReplicator not connected, call Connect() before starting replicator")
	}

	return nil
}
