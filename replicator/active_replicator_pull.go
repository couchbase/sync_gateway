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

var _ ActiveReplicator = &ActivePullReplicator{}

func NewPullReplicator(config *ActiveReplicatorConfig) *ActivePullReplicator {
	return &ActivePullReplicator{
		config:      config,
		blipContext: blip.NewContextCustomID(config.ID+"-pull", blipCBMobileReplication),
	}
}

func (apr *ActivePullReplicator) connect() error {
	if apr == nil {
		return fmt.Errorf("nil ActivePullReplicator, can't connect")
	}

	if apr.blipSender != nil {
		return fmt.Errorf("replicator already has a blipSender, can't connect twice")
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
		return fmt.Errorf("nil ActivePullReplicator, can't start")
	}

	if err := apr.connect(); err != nil {
		return err
	}

	return nil
}
