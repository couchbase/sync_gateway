package replicator

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"

	"github.com/couchbase/go-blip"
	"golang.org/x/net/websocket"
)

// ActiveReplicator is a wrapper to encapsulate separate push and pull active replicators.
type ActiveReplicator struct {
	// push *ActivePushReplicator // TODO: CBG-784
	pull *ActivePullReplicator
}

// NewActiveReplicator returns a bidirectional active replicator for the given config.
func NewActiveReplicator(ctx context.Context, config *ActiveReplicatorConfig) (*ActiveReplicator, error) {
	bar := &ActiveReplicator{}

	// if pushReplication := config.Direction == ActiveReplicatorTypePush || config.Direction == ActiveReplicatorTypePushAndPull; pushReplication {
	// 	bar.Push = NewPushReplicator(config)
	// }

	if pullReplication := config.Direction == ActiveReplicatorTypePull || config.Direction == ActiveReplicatorTypePushAndPull; pullReplication {
		bar.pull = NewPullReplicator(config)
	}

	return bar, nil
}

func (bar *ActiveReplicator) Start() error {
	// if bar.push != nil {
	// 	if err := bar.push.Start(); err != nil {
	// 		return err
	// 	}
	// }

	if bar.pull != nil {
		if err := bar.pull.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (bar *ActiveReplicator) Close() error {
	// if bar.push != nil {
	// 	if err := bar.push.Close(); err != nil {
	// 		return err
	// 	}
	// }

	if bar.pull != nil {
		if err := bar.pull.Close(); err != nil {
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
