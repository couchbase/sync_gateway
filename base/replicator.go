package base

import (
	"errors"
	"net/http"
	"sync"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

const (
	defaultContinuousRetryTime = 500 * time.Millisecond
)

type Replicator struct {
	replications      map[string]sgreplicate.SGReplication
	replicationParams map[string]sgreplicate.ReplicationParameters
	lock              sync.RWMutex
}

type Task struct {
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

// Replicate starts or stops the replication for the given parameters.
func (r *Replicator) Replicate(params sgreplicate.ReplicationParameters, isCancel bool) (*Task, error) {
	if isCancel {
		return r.stopReplication(params)
	} else {
		return r.startReplication(params)
	}
}

// ActiveTasks returns the tasks for active replications.
func (r *Replicator) ActiveTasks() []Task {
	r.lock.RLock()
	defer r.lock.RUnlock()

	tasks := make([]Task, 0, len(r.replications))

	for repID, replication := range r.replications {
		params := r.replicationParams[repID]
		task := taskForReplication(replication, params)
		tasks = append(tasks, *task)
	}

	return tasks
}

// StopReplications stops all active replications.
func (r *Replicator) StopReplications() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	for id, rep := range r.replications {
		LogTo("Replicate", "Stopping replication %s", id)
		if err := rep.Stop(); err != nil {
			Warn("Error stopping replication %s.  It's possible that the replication was already stopped and this can be safely ignored. Error: %v.", id, err)
		}
		LogTo("Replicate", "Stopped replication %s", id)
	}

	r.replications = make(map[string]sgreplicate.SGReplication)
	r.replicationParams = make(map[string]sgreplicate.ReplicationParameters)

	return nil
}

// Starts a replication based on the provided replication config.
func (r *Replicator) startReplication(parameters sgreplicate.ReplicationParameters) (*Task, error) {
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
		replication, err = r.runOneShotReplication(parameters)
	case sgreplicate.CONTINUOUS:
		replication, err = r.runContinuousReplication(parameters)
	default:
		err = errors.New("Unknown replication lifecycle")
	}

	if err != nil {
		return nil, err
	}

	return taskForReplication(replication, parameters), nil
}

func (r *Replicator) runOneShotReplication(parameters sgreplicate.ReplicationParameters) (sgreplicate.SGReplication, error) {
	r.lock.Lock()

	_, found := r._findReplication(parameters)
	if found {
		r.lock.Unlock()
		return nil, HTTPErrorf(http.StatusConflict, "Replication already active for specified parameters")
	}

	replication := sgreplicate.StartOneShotReplication(parameters)
	r._addReplication(replication, parameters)
	r.lock.Unlock()

	LogTo("Replicate", "Started one-shot replication: %v", replication)

	if parameters.Async {
		go func() {
			defer r.removeReplication(parameters.ReplicationId)
			if _, err := replication.WaitUntilDone(); err != nil {
				Warn("async one-shot replication %s failed: %v", parameters.ReplicationId, err)
			}
		}()
		return replication, nil
	}

	_, err := replication.WaitUntilDone()
	r.removeReplication(parameters.ReplicationId)
	return replication, err
}

func (r *Replicator) runContinuousReplication(parameters sgreplicate.ReplicationParameters) (sgreplicate.SGReplication, error) {
	r.lock.Lock()

	_, found := r._findReplication(parameters)
	if found {
		r.lock.Unlock()
		return nil, HTTPErrorf(http.StatusConflict, "Replication already active for specified parameters")
	}

	notificationChan := make(chan sgreplicate.ContinuousReplicationNotification)

	factory := func(parameters sgreplicate.ReplicationParameters, notificationChan chan sgreplicate.ReplicationNotification) sgreplicate.Runnable {
		parameters.Lifecycle = sgreplicate.ONE_SHOT
		return sgreplicate.NewReplication(parameters, notificationChan)
	}

	replication := sgreplicate.NewContinuousReplication(parameters, factory, notificationChan, defaultContinuousRetryTime)
	r._addReplication(replication, parameters)
	r.lock.Unlock()

	LogTo("Replicate", "Started continuous replication: %v", replication)

	// Start goroutine to monitor notification channel, to remove the replication if it's terminated internally by sg-replicate
	go func(rep sgreplicate.SGReplication, notificationChan chan sgreplicate.ContinuousReplicationNotification) {
		defer r.removeReplication(parameters.ReplicationId)

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

func (r *Replicator) stopReplication(parameters sgreplicate.ReplicationParameters) (*Task, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	repID, found := r._findReplication(parameters)
	if !found {
		return nil, HTTPErrorf(http.StatusNotFound, "No replication found matching specified parameters")
	}

	replication := r.replications[repID]
	parameters = r.replicationParams[repID]

	if err := replication.Stop(); err != nil {
		return nil, err
	}

	delete(r.replications, repID)
	delete(r.replicationParams, repID)

	return taskForReplication(replication, parameters), nil
}

// removeReplication removes the given replicaiton from the replicator maps.
func (r *Replicator) removeReplication(repID string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.replications, repID)
	delete(r.replicationParams, repID)
}

// _addReplication adds the given replicaiton to the replicator maps.
func (r *Replicator) _addReplication(rep sgreplicate.SGReplication, parameters sgreplicate.ReplicationParameters) {
	r.replications[parameters.ReplicationId] = rep
	r.replicationParams[parameters.ReplicationId] = parameters
}

// _findReplication will search for replications with equal parameters and return the ID if found.
func (r *Replicator) _findReplication(queryParams sgreplicate.ReplicationParameters) (repID string, found bool) {
	for _, repParams := range r.replicationParams {
		// match on ID if provided
		if queryParams.ReplicationId != "" && queryParams.ReplicationId == repParams.ReplicationId {
			return repParams.ReplicationId, true
		}
		if repParams.Equals(queryParams) {
			return repParams.ReplicationId, true
		}
	}
	return "", false
}

// taskForReplication returns the task for the given replication.
func taskForReplication(replication sgreplicate.SGReplication, params sgreplicate.ReplicationParameters) *Task {
	stats := replication.GetStats()
	return &Task{
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
}
