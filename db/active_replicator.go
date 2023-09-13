/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// ActiveReplicator is a wrapper to encapsulate separate push and pull active replicators.
type ActiveReplicator struct {
	ID     string
	Push   *ActivePushReplicator
	Pull   *ActivePullReplicator
	config *ActiveReplicatorConfig
}

// NewActiveReplicator returns a bidirectional active replicator for the given config.
func NewActiveReplicator(ctx context.Context, config *ActiveReplicatorConfig) (*ActiveReplicator, error) {
	ar := &ActiveReplicator{
		ID:     config.ID,
		config: config,
	}

	if pushReplication := config.Direction == ActiveReplicatorTypePush || config.Direction == ActiveReplicatorTypePushAndPull; pushReplication {
		var err error
		ar.Push, err = NewPushReplicator(ctx, config)
		if err != nil {
			return nil, err
		}
		if ar.config.onComplete != nil {
			ar.Push.onReplicatorComplete = ar._onReplicationComplete
		}
	}

	if pullReplication := config.Direction == ActiveReplicatorTypePull || config.Direction == ActiveReplicatorTypePushAndPull; pullReplication {
		var err error
		ar.Pull, err = NewPullReplicator(ctx, config)
		if err != nil {
			return nil, err
		}
		if ar.config.onComplete != nil {
			ar.Pull.onReplicatorComplete = ar._onReplicationComplete
		}
	}

	base.InfofCtx(ctx, base.KeyReplicate, "Created active replicator ID:%s", config.ID)
	return ar, nil
}

func (ar *ActiveReplicator) Start(ctx context.Context) error {

	if ar.Push == nil && ar.Pull == nil {
		return fmt.Errorf("Attempted to start activeReplicator for %s with neither Push nor Pull defined", base.UD(ar.ID))
	}

	var pushErr error
	if ar.Push != nil {
		pushErr = ar.Push.Start(ctx)
	}

	var pullErr error
	if ar.Pull != nil {
		pullErr = ar.Pull.Start(ctx)
	}

	if pushErr != nil {
		return pushErr
	}

	if pullErr != nil {
		return pullErr
	}

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

	if base.BoolDefault(ar.config.reportHandlerPanicsOnStop, true) {
		if stats := ar.config.ReplicationStatsMap; stats != nil {
			if val := stats.NumHandlersPanicked.Value(); val > 0 {
				return fmt.Errorf("%d handlers panicked", val)
			}
		}
	}

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

	if ar.config.ReplicationStatsMap != nil {
		ar.config.ReplicationStatsMap.Reset()
	}

	return nil
}

// _onReplicationComplete is invoked from Complete in an active replication.  If all replications
// associated with the ActiveReplicator are complete, onComplete is invoked
func (ar *ActiveReplicator) _onReplicationComplete() {
	allReplicationsComplete := true
	if ar.Push != nil && ar.Push.getState() != ReplicationStateStopped {
		allReplicationsComplete = false
	}
	if ar.Pull != nil && ar.Pull.getState() != ReplicationStateStopped {
		allReplicationsComplete = false
	}

	if allReplicationsComplete {
		ar.config.onComplete(ar.ID)
	}

}

func (ar *ActiveReplicator) State(ctx context.Context) (state string, errorMessage string) {

	state = ReplicationStateStopped
	if ar.Push != nil {
		state, errorMessage = ar.Push.getStateWithErrorMessage()
	}

	if ar.Pull != nil {
		pullState, pullErrorMessage := ar.Pull.getStateWithErrorMessage()
		state = combinedState(ctx, state, pullState)
		if pullErrorMessage != "" {
			errorMessage = pullErrorMessage
		}
	}

	return state, errorMessage
}

func (ar *ActiveReplicator) GetStatus(ctx context.Context) *ReplicationStatus {

	status := &ReplicationStatus{
		ID: ar.ID,
	}
	status.Status, status.ErrorMessage = ar.State(ctx)

	if ar.Pull != nil {
		status.PullReplicationStatus = ar.Pull.GetStatus().PullReplicationStatus
	}

	if ar.Push != nil {
		status.PushReplicationStatus = ar.Push.GetStatus().PushReplicationStatus
	}

	return status
}

func connect(arc *activeReplicatorCommon, idSuffix string) (blipSender *blip.Sender, bsc *BlipSyncContext, err error) {
	arc.replicationStats.NumConnectAttempts.Add(1)

	blipContext, err := NewSGBlipContext(arc.ctx, arc.config.ID+idSuffix)
	if err != nil {
		return nil, nil, err
	}
	blipContext.WebsocketPingInterval = arc.config.WebsocketPingInterval
	blipContext.OnExitCallback = func() {
		// fall into a reconnect loop only if the connection is unexpectedly closed.
		if arc.ctx.Err() == nil {
			go arc.reconnectLoop()
		}
	}

	bsc = NewBlipSyncContext(arc.ctx, blipContext, arc.config.ActiveDB, blipContext.ID, arc.replicationStats)

	bsc.loggingCtx = base.CorrelationIDLogCtx(
		base.DatabaseLogCtx(base.NewNonCancelCtx().Ctx, arc.config.ActiveDB.Name, nil),
		arc.config.ID+idSuffix)

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
	client := base.GetHttpClientForWebSocket(insecureSkipVerify)
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

	// Strip userinfo from the URL, don't need it because of the Basic auth header.
	var basicAuthCreds *url.Userinfo
	if target.User != nil {
		// take a copy
		if password, hasPassword := target.User.Password(); hasPassword {
			basicAuthCreds = url.UserPassword(target.User.Username(), password)
		} else {
			basicAuthCreds = url.User(target.User.Username())
		}
		target.User = nil
	}

	config := blip.DialOptions{
		URL:        target.String() + "/_blipsync?" + BLIPSyncClientTypeQueryParam + "=" + string(BLIPClientTypeSGR2),
		HTTPClient: client,
	}

	if basicAuthCreds != nil {
		config.HTTPHeader = http.Header{
			"Authorization": []string{"Basic " + base64UserInfo(basicAuthCreds)},
		}
	}

	return blipContext.DialConfig(&config)
}

// base64UserInfo returns the base64 encoded version of the given UserInfo.
// Can't use i.String() here because that returns URL encoded versions of credentials.
func base64UserInfo(i *url.Userinfo) string {
	password, _ := i.Password()
	return base64.StdEncoding.EncodeToString([]byte(i.Username() + ":" + password))
}

// combinedState reports a combined replication state for a pushAndPull
// replication, based on the following criteria:
//   - if either replication is in error, return error
//   - if either replication is running, return running
//   - if both replications are stopped, return stopped
func combinedState(ctx context.Context, state1, state2 string) (combinedState string) {
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

	base.InfofCtx(ctx, base.KeyReplicate, "Unhandled combination of replication states (%s, %s), returning %s", state1, state2, state1)
	return state1
}

func (ar *ActiveReplicator) purgeCheckpoints() {
	if ar.Pull != nil {
		_ = ar.Pull.reset()
	}

	if ar.Push != nil {
		_ = ar.Push.reset()
	}
}

// LoadReplicationStatus attempts to load both push and pull replication checkpoints, and constructs the combined status
func LoadReplicationStatus(ctx context.Context, dbContext *DatabaseContext, replicationID string) (status *ReplicationStatus, err error) {

	status = &ReplicationStatus{
		ID: replicationID,
	}

	pullStatusKey := dbContext.MetadataKeys.ReplicationStatusKey(PullCheckpointID(replicationID))
	pullStatus, _ := getLocalStatus(ctx, dbContext.MetadataStore, pullStatusKey)
	if pullStatus != nil {
		status.PullReplicationStatus = pullStatus.PullReplicationStatus
		status.Status = pullStatus.Status
		status.ErrorMessage = pullStatus.ErrorMessage
		status.LastSeqPull = pullStatus.LastSeqPull
	}

	pushStatusKey := dbContext.MetadataKeys.ReplicationStatusKey(PushCheckpointID(replicationID))
	pushStatus, _ := getLocalStatus(ctx, dbContext.MetadataStore, pushStatusKey)
	if pushStatus != nil {
		status.PushReplicationStatus = pushStatus.PushReplicationStatus
		status.Status = pushStatus.Status
		status.ErrorMessage = pushStatus.ErrorMessage
		status.LastSeqPush = pushStatus.LastSeqPush
	}

	if pullStatus == nil && pushStatus == nil {
		return nil, errors.New("Replication status not found")
	}

	return status, nil
}

func PushCheckpointID(replicationID string) string {
	return "sgr2cp:push:" + replicationID
}

func PullCheckpointID(replicationID string) string {
	return "sgr2cp:pull:" + replicationID
}
