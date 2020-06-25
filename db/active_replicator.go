package db

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/couchbase/go-blip"
	"golang.org/x/net/websocket"
)

const (
	// TODO: Not aligned these to any stats in the PRD yet, these are used for test assertions.
	ActiveReplicatorStatsKeyRevsReceivedTotal        = "revs_received_total"
	ActiveReplicatorStatsKeyChangesRevsReceivedTotal = "changes_revs_received_total"
	ActiveReplicatorStatsKeyRevsSentTotal            = "revs_sent_total"
	ActiveReplicatorStatsKeyRevsRequestedTotal       = "revs_requested_total"
)

const (
	defaultCheckpointInterval = time.Second * 30
)

// ActiveReplicator is a wrapper to encapsulate separate push and pull active replicators.
type ActiveReplicator struct {
	ID   string
	Push *ActivePushReplicator
	Pull *ActivePullReplicator
}

// NewActiveReplicator returns a bidirectional active replicator for the given config.
func NewActiveReplicator(ctx context.Context, config *ActiveReplicatorConfig) (*ActiveReplicator, error) {
	ar := &ActiveReplicator{
		ID: config.ID,
	}

	if pushReplication := config.Direction == ActiveReplicatorTypePush || config.Direction == ActiveReplicatorTypePushAndPull; pushReplication {
		ar.Push = NewPushReplicator(ctx, config)
	}

	if pullReplication := config.Direction == ActiveReplicatorTypePull || config.Direction == ActiveReplicatorTypePushAndPull; pullReplication {
		ar.Pull = NewPullReplicator(ctx, config)
	}

	return ar, nil
}

func (ar *ActiveReplicator) Start() error {
	if ar.Push != nil {
		if err := ar.Push.Start(); err != nil {
			return err
		}
	}

	if ar.Pull != nil {
		if err := ar.Pull.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (ar *ActiveReplicator) Close() error {
	if ar.Push != nil {
		if err := ar.Push.Close(); err != nil {
			return err
		}
	}

	if ar.Pull != nil {
		if err := ar.Pull.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (ar *ActiveReplicator) GetStatus() *ReplicationStatus {

	status := &ReplicationStatus{
		ID:     ar.ID,
		Status: "running",
	}
	if ar.Pull != nil {
		pullStats := ar.Pull.blipSyncContext.replicationStats
		status.DocsRead = pullStats.HandleRevCount.Value()
		status.DocsPurged = pullStats.DocsPurgedCount.Value()
		status.RejectedLocal = pullStats.HandleRevErrorCount.Value()
		status.LastSeqPull = ar.Pull.Checkpointer.lastCheckpointSeq
	}

	if ar.Push != nil {
		pushStats := ar.Push.blipSyncContext.replicationStats
		status.DocsWritten = pushStats.SendRevCount.Value()
		status.DocWriteFailures = pushStats.SendRevErrorCount.Value()
		// TODO: This is another scenario where we need to send a rev without noreply set to get the returned error
		// status.RejectedRemote = pushStats.SendRevSyncFunctionErrorCount.Value()
	}

	return status
}

func connect(idSuffix string, config *ActiveReplicatorConfig) (blipSender *blip.Sender, bsc *BlipSyncContext, err error) {

	blipContext := blip.NewContextCustomID(config.ID+idSuffix, blipCBMobileReplication)
	bsc = NewBlipSyncContext(blipContext, config.ActiveDB, blipContext.ID)

	blipSender, err = blipSync(*config.PassiveDBURL, blipContext)
	if err != nil {
		return nil, nil, err
	}

	return blipSender, bsc, nil
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
