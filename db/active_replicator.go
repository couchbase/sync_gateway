package db

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"

	"github.com/couchbase/go-blip"
	"golang.org/x/net/websocket"
)

const (
	// TODO: Not aligned these to any stats in the PRD yet, these are used for test assertions.
	ActiveReplicatorStatsKeyGetCheckpointHitTotal    = "get_checkpoint_hit_total"
	ActiveReplicatorStatsKeyGetCheckpointMissTotal   = "get_checkpoint_miss_total"
	ActiveReplicatorStatsKeySetCheckpointTotal       = "set_checkpoint_total"
	ActiveReplicatorStatsKeyRevsReceivedTotal        = "revs_received_total"
	ActiveReplicatorStatsKeyChangesRevsReceivedTotal = "changes_revs_received_total"
)

// ActiveReplicator is a wrapper to encapsulate separate push and pull active replicators.
type ActiveReplicator struct {
	// push *ActivePushReplicator // TODO: CBG-784
	Pull *ActivePullReplicator
}

// NewActiveReplicator returns a bidirectional active replicator for the given config.
func NewActiveReplicator(ctx context.Context, config *ActiveReplicatorConfig) (*ActiveReplicator, error) {
	ar := &ActiveReplicator{}

	// if pushReplication := config.Direction == ActiveReplicatorTypePush || config.Direction == ActiveReplicatorTypePushAndPull; pushReplication {
	// 	ar.Push = NewPushReplicator(config)
	// }

	if pullReplication := config.Direction == ActiveReplicatorTypePull || config.Direction == ActiveReplicatorTypePushAndPull; pullReplication {
		ar.Pull = NewPullReplicator(ctx, config)
	}

	return ar, nil
}

func (ar *ActiveReplicator) Start() error {
	// if ar.push != nil {
	// 	if err := ar.push.Start(); err != nil {
	// 		return err
	// 	}
	// }

	if ar.Pull != nil {
		if err := ar.Pull.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (ar *ActiveReplicator) Close() error {
	// if ar.push != nil {
	// 	if err := ar.push.Close(); err != nil {
	// 		return err
	// 	}
	// }

	if ar.Pull != nil {
		if err := ar.Pull.Close(); err != nil {
			return err
		}
	}

	return nil
}

// blipSync opens a connection to the target, and returns a blip.Sender to send messages over.
func blipSync(target url.URL, blipContext *blip.Context) (*blip.Sender, error) {
	// GET target database endpoint to see if reachable for exit-early/clearer error message
	resp, err := http.Get(target.String())
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d from target database", resp.StatusCode)
	}

	// switch to websocket protocol scheme
	if target.Scheme == "http" {
		target.Scheme = "ws"
	} else if target.Scheme == "https" {
		target.Scheme = "wss"
	}

	config, err := websocket.NewConfig(target.String()+"/_blipsync", "http://localhost")
	if err != nil {
		return nil, err
	}

	if target.User != nil {
		config.Header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(target.User.String())))
	}

	return blipContext.DialConfig(config)
}
