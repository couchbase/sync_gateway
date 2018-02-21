package base

import (
	"errors"
	"net/http"
	"sync"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

const (
	DefaultContinuousRetryTimeMs = 500
)

type Replicator struct {
	replications      map[string]sgreplicate.SGReplication
	replicationParams map[string]sgreplicate.ReplicationParameters
	lock              sync.RWMutex
}

type ActiveTask struct {
	TaskType         string      `json:"type"`
	ReplicationID    string      `json:"replication_id"`
	Continuous       bool        `json:"continuous"`
	Source           string      `json:"source"`
	Target           string      `json:"target"`
	DocsRead         uint32      `json:"docs_read"`
	DocsWritten      uint32      `json:"docs_written"`
	DocWriteFailures uint32      `json:"doc_write_failures"`
	StartLastSeq     uint32      `json:"start_last_seq"`
	EndLastSeq       interface{} `json:"end_last_seq"`
}

func NewReplicator() *Replicator {
	return &Replicator{
		replications:      make(map[string]sgreplicate.SGReplication),
		replicationParams: make(map[string]sgreplicate.ReplicationParameters),
	}
}

func (r *Replicator) Replicate(params sgreplicate.ReplicationParameters, isCancel bool) (task *ActiveTask, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if isCancel {
		return r.stopReplication(params)
	} else {
		return r.startReplication(params)
	}
}

func (r *Replicator) ActiveTasks() (tasks []ActiveTask) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	tasks = make([]ActiveTask, 0)
	for replicationId, replication := range r.replications {
		params := r.replicationParams[replicationId]
		task := r.populateActiveTaskFromReplication(replication, params)
		tasks = append(tasks, *task)
	}
	return tasks

}

func (r *Replicator) addReplication(rep sgreplicate.SGReplication, parameters sgreplicate.ReplicationParameters) {
	r.replications[parameters.ReplicationId] = rep
	r.replicationParams[parameters.ReplicationId] = parameters
}

func (r *Replicator) getReplication(repId string) sgreplicate.SGReplication {
	if rep, ok := r.replications[repId]; ok {
		return rep
	} else {
		return nil
	}
}

func (r *Replicator) getReplicationParams(repId string) sgreplicate.ReplicationParameters {
	if params, ok := r.replicationParams[repId]; ok {
		return params
	} else {
		return sgreplicate.ReplicationParameters{}
	}
}

func (r *Replicator) getReplicationForParams(queryParams sgreplicate.ReplicationParameters) (replicationId string, found bool) {

	// Iterate over the known replications looking for a match
	for knownReplicationId, _ := range r.replications {

		repParams := r.replicationParams[knownReplicationId]

		if queryParams.ReplicationId != "" && queryParams.ReplicationId == repParams.ReplicationId {
			return repParams.ReplicationId, true
		}

		if repParams.Equals(queryParams) {
			return repParams.ReplicationId, true
		}

	}
	return "", false

}

func (r *Replicator) removeReplication(repId string) {
	delete(r.replications, repId)
	delete(r.replicationParams, repId)
}

func (r *Replicator) removeReplicationLock(repId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.removeReplication(repId)
}

// Starts a replication based on the provided replication config.
func (r *Replicator) startReplication(parameters sgreplicate.ReplicationParameters) (*ActiveTask, error) {
	_, found := r.getReplicationForParams(parameters)
	if found {
		return nil, HTTPErrorf(http.StatusConflict, "Replication already active for specified parameters")
	}

	// Generate ID if blank for the new replication
	if parameters.ReplicationId == "" {
		parameters.ReplicationId = CreateUUID()
	}

	LogTo("Replicate", "Creating replication with parameters %+v", parameters)

	var (
		replication sgreplicate.SGReplication
		err         error
	)

	switch parameters.Lifecycle {
	case sgreplicate.ONE_SHOT:
		replication, err = r.startOneShotReplication(parameters)
	case sgreplicate.CONTINUOUS:
		replication, err = r.startContinuousReplication(parameters)
	default:
		err = errors.New("Unknown replication lifecycle")
	}

	if err != nil {
		return nil, err
	}

	return r.populateActiveTaskFromReplication(replication, parameters), nil
}

func (r *Replicator) stopReplication(parameters sgreplicate.ReplicationParameters) (task *ActiveTask, err error) {
	repId, found := r.getReplicationForParams(parameters)
	if !found {
		return nil, HTTPErrorf(http.StatusNotFound, "No replication found matching specified parameters")
	}

	replication := r.getReplication(repId)
	parameters = r.getReplicationParams(repId)

	if replication == nil {
		return nil, HTTPErrorf(http.StatusNotFound, "No replication found matching specified replication ID")
	}
	err = replication.Stop()
	if err != nil {
		return nil, err
	}

	taskState := r.populateActiveTaskFromReplication(replication, parameters)

	r.removeReplication(repId)
	return taskState, nil
}

func (r *Replicator) startOneShotReplication(parameters sgreplicate.ReplicationParameters) (sgreplicate.SGReplication, error) {

	replication := sgreplicate.StartOneShotReplication(parameters)
	r.addReplication(replication, parameters)

	if parameters.Async {
		go func() {
			defer r.removeReplicationLock(parameters.ReplicationId)
			r.runOneShotReplication(replication, parameters)
		}()
		return replication, nil
	} else {
		err := r.runOneShotReplication(replication, parameters)
		r.removeReplication(parameters.ReplicationId)
		return replication, err
	}
}

// Calls WaitUntilDone to work the notification channel for the one-shot replication.  Used for both synchronous and async one-shot replications.
func (r *Replicator) runOneShotReplication(replication *sgreplicate.Replication, parameters sgreplicate.ReplicationParameters) error {
	_, err := replication.WaitUntilDone()
	return err
}

func (r *Replicator) startContinuousReplication(parameters sgreplicate.ReplicationParameters) (sgreplicate.SGReplication, error) {

	notificationChan := make(chan sgreplicate.ContinuousReplicationNotification)

	factory := func(parameters sgreplicate.ReplicationParameters, notificationChan chan sgreplicate.ReplicationNotification) sgreplicate.Runnable {
		parameters.Lifecycle = sgreplicate.ONE_SHOT
		return sgreplicate.NewReplication(parameters, notificationChan)
	}

	retryTime := time.Millisecond * time.Duration(DefaultContinuousRetryTimeMs)
	replication := sgreplicate.NewContinuousReplication(parameters, factory, notificationChan, retryTime)
	r.addReplication(replication, parameters)
	LogTo("Replicate", "Started continuous replication: %v", replication)

	// Start goroutine to monitor notification channel, to remove the replication if it's terminated internally by sg-replicate
	go func(rep sgreplicate.SGReplication, notificationChan chan sgreplicate.ContinuousReplicationNotification) {
		defer r.removeReplicationLock(parameters.ReplicationId)

		for {
			select {
			case notification, ok := <-notificationChan:
				if !ok {
					LogTo("Replicate", "Replication %s was terminated.", parameters.ReplicationId)
					return
				}
				LogTo("Replicate+", "Got notification %v", notification)
			}
		}
	}(replication, notificationChan)

	return replication, nil
}

func (r *Replicator) populateActiveTaskFromReplication(replication sgreplicate.SGReplication, params sgreplicate.ReplicationParameters) (task *ActiveTask) {

	stats := replication.GetStats()

	task = &ActiveTask{
		TaskType:         "replication",
		ReplicationID:    params.ReplicationId,
		Source:           params.GetSourceDbUrl(),
		Target:           params.GetTargetDbUrl(),
		Continuous:       params.Lifecycle == sgreplicate.CONTINUOUS,
		DocsRead:         stats.GetDocsRead(),
		DocsWritten:      stats.GetDocsWritten(),
		DocWriteFailures: stats.GetDocWriteFailures(),
		StartLastSeq:     stats.GetStartLastSeq(),
		EndLastSeq:       stats.GetEndLastSeq(),
	}

	return
}

// StopReplications stops all replications.
func (r *Replicator) StopReplications() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, p := range r.replicationParams {
		LogTo("Replicate", "Stopping replication %s", p.ReplicationId)
		if _, err := r.stopReplication(p); err != nil {
			Warn("Error stopping replication %s.  It's possible that the replication was already stopped and this can be safely ignored. Error: %v.", p.ReplicationId, err)
		}
		LogTo("Replicate", "Stopped replication %s", p.ReplicationId)
	}

	return nil
}
