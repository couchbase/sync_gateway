package db

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
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
func NewActiveReplicator(config *ActiveReplicatorConfig) *ActiveReplicator {
	ar := &ActiveReplicator{
		ID: config.ID,
	}

	if pushReplication := config.Direction == ActiveReplicatorTypePush || config.Direction == ActiveReplicatorTypePushAndPull; pushReplication {
		ar.Push = NewPushReplicator(config)
	}

	if pullReplication := config.Direction == ActiveReplicatorTypePull || config.Direction == ActiveReplicatorTypePushAndPull; pullReplication {
		ar.Pull = NewPullReplicator(config)
	}

	return ar
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

func (ar *ActiveReplicator) Stop() error {
	if ar.Push != nil {
		if err := ar.Push.Stop(); err != nil {
			return err
		}
	}

	if ar.Pull != nil {
		if err := ar.Pull.Stop(); err != nil {
			return err
		}
	}

	return nil
}

func (ar *ActiveReplicator) Reset() error {
	if ar.Push != nil {
		if err := ar.Push.Reset(); err != nil {
			return err
		}
	}

	if ar.Pull != nil {
		if err := ar.Pull.Reset(); err != nil {
			return err
		}
	}

	return nil
}

func (ar *ActiveReplicator) State() (state string, errorMessage string) {

	if ar.Push != nil {
		state = ar.Push.state
		if ar.Push.state == ReplicationStateError && ar.Push.lastError != nil {
			errorMessage = ar.Push.lastError.Error()
		}
	}

	if ar.Pull != nil {
		state = combinedState(state, ar.Pull.state)
		if ar.Pull.state == ReplicationStateError && ar.Pull.lastError != nil {
			errorMessage = ar.Pull.lastError.Error()
		}
	}

	return state, errorMessage
}

func (ar *ActiveReplicator) GetStatus() *ReplicationStatus {

	status := &ReplicationStatus{
		ID: ar.ID,
	}
	status.Status, status.ErrorMessage = ar.State()

	if ar.Pull != nil {
		pullStats := ar.Pull.replicationStats
		status.DocsRead = pullStats.HandleRevCount.Value()
		status.DocsPurged = pullStats.DocsPurgedCount.Value()
		status.RejectedLocal = pullStats.HandleRevErrorCount.Value()
		if ar.Pull.Checkpointer != nil {
			status.LastSeqPull = ar.Pull.Checkpointer.calculateSafeProcessedSeq()
		}
	}

	if ar.Push != nil {
		pushStats := ar.Push.replicationStats
		status.DocsWritten = pushStats.SendRevCount.Value()
		status.DocWriteFailures = pushStats.SendRevErrorCount.Value()
		if ar.Push.Checkpointer != nil {
			status.LastSeqPush = ar.Push.Checkpointer.calculateSafeProcessedSeq()
		}
		// TODO: This is another scenario where we need to send a rev without noreply set to get the returned error
		// status.RejectedRemote = pushStats.SendRevSyncFunctionErrorCount.Value()
	}

	return status
}

func connect(idSuffix string, config *ActiveReplicatorConfig, replicationStats *BlipSyncStats) (blipSender *blip.Sender, bsc *BlipSyncContext, err error) {

	blipContext := NewSGBlipContext(context.TODO(), config.ID+idSuffix)
	blipContext.WebsocketPingInterval = config.WebsocketPingInterval
	bsc = NewBlipSyncContext(blipContext, config.ActiveDB, blipContext.ID)
	bsc.loggingCtx = context.WithValue(context.Background(), base.LogContextKey{},
		base.LogContext{CorrelationID: config.ID + idSuffix},
	)
	bsc.InitializeStats(replicationStats)

	// NewBlipSyncContext has already set deltas as disabled/enabled based on config.ActiveDB.
	// If deltas have been disabled in the replication config, override this value
	if config.DeltasEnabled == false {
		bsc.sgCanUseDeltas = false
	}

	blipSender, err = blipSync(*config.PassiveDBURL, blipContext, config.InsecureSkipVerify)
	if err != nil {
		return nil, nil, err
	}

	return blipSender, bsc, nil
}

// blipSync opens a connection to the target, and returns a blip.Sender to send messages over.
func blipSync(target url.URL, blipContext *blip.Context, insecureSkipVerify bool) (*blip.Sender, error) {
	// GET target database endpoint to see if reachable for exit-early/clearer error message
	req, err := http.NewRequest(http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, err
	}
	client := base.GetHttpClient(insecureSkipVerify)
	resp, err := client.Do(req)
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

	if insecureSkipVerify {
		if config.TlsConfig == nil {
			config.TlsConfig = new(tls.Config)
		}
		config.TlsConfig.InsecureSkipVerify = true
	}

	if target.User != nil {
		config.Header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(target.User.String())))
	}

	return blipContext.DialConfig(config)
}

// combinedState reports a combined replication state for a pushAndPull
// replication, based on the following criteria:
//   - if either replication is in error, return error
//   - if either replication is running, return running
//   - if both replications are stopped, return stopped
func combinedState(state1, state2 string) (combinedState string) {
	if state1 == "" {
		return state2
	}
	if state2 == "" {
		return state1
	}

	if state1 == ReplicationStateError || state2 == ReplicationStateError {
		return ReplicationStateError
	}

	if state1 == ReplicationStateRunning || state2 == ReplicationStateRunning {
		return ReplicationStateRunning
	}

	if state1 == ReplicationStateStopped && state2 == ReplicationStateStopped {
		return ReplicationStateStopped
	}

	base.Infof(base.KeyReplicate, "Unhandled combination of replication states (%s, %s), returning %s", state1, state2, state1)
	return state1
}
