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
	defaultCheckpointInterval    = time.Second * 30
	defaultPublishStatusInterval = time.Second * 10
)

// ActiveReplicator is a wrapper to encapsulate separate push and pull active replicators.
type ActiveReplicator struct {
	ID        string
	Push      *ActivePushReplicator
	Pull      *ActivePullReplicator
	config    *ActiveReplicatorConfig
	statusKey string // key used when persisting replication status
}

// NewActiveReplicator returns a bidirectional active replicator for the given config.
func NewActiveReplicator(config *ActiveReplicatorConfig) *ActiveReplicator {
	ar := &ActiveReplicator{
		ID:        config.ID,
		config:    config,
		statusKey: replicationStatusKey(config.ID),
	}

	if pushReplication := config.Direction == ActiveReplicatorTypePush || config.Direction == ActiveReplicatorTypePushAndPull; pushReplication {
		ar.Push = NewPushReplicator(config)
		if ar.config.onComplete != nil {
			ar.Push.onReplicatorComplete = ar._onReplicationComplete
		}
	}

	if pullReplication := config.Direction == ActiveReplicatorTypePull || config.Direction == ActiveReplicatorTypePushAndPull; pullReplication {
		ar.Pull = NewPullReplicator(config)
		if ar.config.onComplete != nil {
			ar.Pull.onReplicatorComplete = ar._onReplicationComplete
		}
	}

	base.InfofCtx(config.ActiveDB.Ctx, base.KeyReplicate, "Created active replicator ID:%s statusKey: %s", config.ID, ar.statusKey)
	return ar
}

func (ar *ActiveReplicator) Start() error {

	if ar.Push == nil && ar.Pull == nil {
		return fmt.Errorf("Attempted to start activeReplicator for %s with neither Push nor Pull defined", base.UD(ar.ID))
	}

	var pushErr error
	if ar.Push != nil {
		pushErr = ar.Push.Start()
	}

	var pullErr error
	if ar.Pull != nil {
		pullErr = ar.Pull.Start()
	}

	if pushErr != nil {
		return pushErr
	}

	if pullErr != nil {
		return pullErr
	}

	_ = ar.publishStatus()

	return nil
}

func (ar *ActiveReplicator) Stop() error {

	if ar.Push == nil && ar.Pull == nil {
		return fmt.Errorf("Attempted to stop activeReplicator for %s with neither Push nor Pull defined", base.UD(ar.ID))
	}

	var pushErr error
	if ar.Push != nil {
		pushErr = ar.Push.Stop()
	}

	var pullErr error
	if ar.Pull != nil {
		pullErr = ar.Pull.Stop()
	}

	if pushErr != nil {
		return pushErr
	}

	if pullErr != nil {
		return pullErr
	}

	_ = ar.publishStatus()
	return nil
}

func (ar *ActiveReplicator) Reset() error {
	var pushErr error
	if ar.Push != nil {
		pushErr = ar.Push.reset()
	}

	var pullErr error
	if ar.Pull != nil {
		pullErr = ar.Pull.reset()
	}

	if pushErr != nil {
		return pushErr
	}

	if pullErr != nil {
		return pullErr
	}

	err := ar.purgeStatus()
	if err != nil {
		base.Warnf("Unable to purge replication status for reset replication: %v", err)
	}
	return nil
}

// _onReplicationComplete is invoked from Complete in an active replication.  If all replications
// associated with the ActiveReplicator are complete, onComplete is invoked
func (ar *ActiveReplicator) _onReplicationComplete() {
	allReplicationsComplete := true
	if ar.Push != nil && ar.Push.state != ReplicationStateStopped {
		allReplicationsComplete = false
	}
	if ar.Pull != nil && ar.Pull.state != ReplicationStateStopped {
		allReplicationsComplete = false
	}

	if allReplicationsComplete {
		_ = ar.publishStatus()
		ar.config.onComplete(ar.ID)
	}

}

func (ar *ActiveReplicator) State() (state string, errorMessage string) {

	state = ReplicationStateStopped
	if ar.Push != nil {
		state, errorMessage = ar.Push.getStateWithErrorMessage()
	}

	if ar.Pull != nil {
		pullState, pullErrorMessage := ar.Pull.getStateWithErrorMessage()
		state = combinedState(state, pullState)
		if pullErrorMessage != "" {
			errorMessage = pullErrorMessage
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
		status.DocsPurged = pullStats.HandleRevDocsPurgedCount.Value()
		status.RejectedLocal = pullStats.HandleRevErrorCount.Value()
		status.DeltasRecv = pullStats.HandleRevDeltaRecvCount.Value()
		status.DeltasRequested = pullStats.HandleChangesDeltaRequestedCount.Value()
		if ar.Pull.Checkpointer != nil {
			status.LastSeqPull = ar.Pull.Checkpointer.calculateSafeProcessedSeq()
		}
	}

	if ar.Push != nil {
		pushStats := ar.Push.replicationStats
		status.DocsWritten = pushStats.SendRevCount.Value()
		status.DocWriteFailures = pushStats.SendRevErrorTotal.Value()
		status.DocWriteConflict = pushStats.SendRevErrorConflictCount.Value()
		status.RejectedRemote = pushStats.SendRevErrorRejectedCount.Value()
		status.DeltasSent = pushStats.SendRevDeltaSentCount.Value()
		if ar.Push.Checkpointer != nil {
			status.LastSeqPush = ar.Push.Checkpointer.calculateSafeProcessedSeq()
		}
	}

	return status
}

func connect(arc *activeReplicatorCommon, idSuffix string) (blipSender *blip.Sender, bsc *BlipSyncContext, err error) {
	arc.replicationStats.NumConnectAttempts.Add(1)

	blipContext := NewSGBlipContext(arc.ctx, arc.config.ID+idSuffix)
	blipContext.WebsocketPingInterval = arc.config.WebsocketPingInterval
	bsc = NewBlipSyncContext(blipContext, arc.config.ActiveDB, blipContext.ID, arc.replicationStats)
	bsc.loggingCtx = context.WithValue(context.Background(), base.LogContextKey{},
		base.LogContext{CorrelationID: arc.config.ID + idSuffix},
	)

	// NewBlipSyncContext has already set deltas as disabled/enabled based on config.ActiveDB.
	// If deltas have been disabled in the replication config, override this value
	if arc.config.DeltasEnabled == false {
		bsc.sgCanUseDeltas = false
	}

	blipSender, err = blipSync(*arc.config.RemoteDBURL, blipContext, arc.config.InsecureSkipVerify)
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

	if state1 == ReplicationStateStopped && state2 == ReplicationStateStopped {
		return ReplicationStateStopped
	}

	if state1 == ReplicationStateRunning || state2 == ReplicationStateRunning {
		return ReplicationStateRunning
	}

	if state1 == ReplicationStateError || state2 == ReplicationStateError {
		return ReplicationStateError
	}

	if state1 == ReplicationStateReconnecting || state2 == ReplicationStateReconnecting {
		return ReplicationStateReconnecting
	}

	base.Infof(base.KeyReplicate, "Unhandled combination of replication states (%s, %s), returning %s", state1, state2, state1)
	return state1
}

func (ar *ActiveReplicator) publishStatus() error {
	status := ar.GetStatus()
	base.Debugf(base.KeyReplicate, "Persisting replication status for replicationID %v (%v)", ar.ID, ar.statusKey)
	err := ar.config.ActiveDB.Bucket.Set(ar.statusKey, 0, status)
	return err
}

func (ar *ActiveReplicator) purgeStatus() error {
	base.Debugf(base.KeyReplicate, "Purging replication status for replicationID %v (%v)", ar.ID, ar.statusKey)
	err := ar.config.ActiveDB.Bucket.Delete(ar.statusKey)
	if !base.IsKeyNotFoundError(ar.config.ActiveDB.Bucket, err) {
		return err
	}
	return nil
}

func LoadReplicationStatus(dbContext *DatabaseContext, replicationID string) (status *ReplicationStatus, err error) {
	_, err = dbContext.Bucket.Get(replicationStatusKey(replicationID), &status)
	return status, err
}

// replicationStatusKey generates the key used to store status information for the given replicationID.  If replicationID
// is 40 characters or longer, a SHA-1 hash of the replicationID is used in the status key.
// If the replicationID is less than 40 characters, the ID can be used directly without worrying about final key length
// or collision with other sha-1 hashes.
func replicationStatusKey(replicationID string) string {
	statusKeyID := replicationID
	if len(statusKeyID) >= 40 {
		statusKeyID = base.Sha1HashString(replicationID, "")
	}
	return fmt.Sprintf("%s%s", base.SGRStatusPrefix, statusKeyID)
}
