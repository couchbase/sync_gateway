package base

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/couchbaselabs/sg-replicate"
)

const (
	DefaultContinuousRetryTimeMs = 500
)

type Replicator struct {
	replications map[string]sgreplicate.SGReplication
	lock         sync.RWMutex
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
		replications: make(map[string]sgreplicate.SGReplication),
	}
}

func (r *Replicator) Replicate(params sgreplicate.ReplicationParameters, isCancel bool) (task *ActiveTask, err error) {
	if isCancel {
		replicationId := params.ReplicationId
		// If replicationId isn't defined in the cancel request, attempt to look up the replication based on source, target
		if replicationId == "" {
			var found bool
			replicationId, found = r.getReplicationForParams(params)
			if !found {
				return nil, HTTPErrorf(http.StatusNotFound, "No replication found matching specified parameters")
			}
		}

		return nil, r.stopReplication(replicationId)

	} else {
		// Check whether specified replication is already active
		_, found := r.getReplicationForParams(params)
		if found {
			return nil, HTTPErrorf(http.StatusConflict, "Replication already active for specified parameters")
		}
		replication, err := r.startReplication(params)
		return populateActiveTaskFromReplication(replication), err
	}
}

func (r *Replicator) ActiveTasks() (tasks []ActiveTask) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	tasks = make([]ActiveTask, 0)
	for _, replication := range r.replications {
		task := populateActiveTaskFromReplication(replication)
		tasks = append(tasks, *task)
	}
	return tasks

}

func (r *Replicator) addReplication(rep sgreplicate.SGReplication) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.replications[rep.GetParameters().ReplicationId] = rep
}

func (r *Replicator) getReplication(repId string) sgreplicate.SGReplication {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if rep, ok := r.replications[repId]; ok {
		return rep
	} else {
		return nil
	}
}

func (r *Replicator) getReplicationForParams(params sgreplicate.ReplicationParameters) (replicationId string, found bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	// Iterate over the known replications looking for a match
	for _, replication := range r.replications {
		repParams := replication.GetParameters()
		if repParams.Equals(params) {
			return repParams.ReplicationId, true
		}
	}
	return "", false
}

func (r *Replicator) removeReplication(repId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.replications, repId)
}

// Starts a replication based on the provided replication config.
func (r *Replicator) startReplication(parameters sgreplicate.ReplicationParameters) (sgreplicate.SGReplication, error) {

	LogTo("Replicate", "Starting replication with parameters %+v", parameters)

	// Generate ID for the new replication, and add to the map of active replications
	if parameters.ReplicationId == "" {
		parameters.ReplicationId = CreateUUID()
	}

	switch parameters.Lifecycle {
	case sgreplicate.ONE_SHOT:
		return r.startOneShotReplication(parameters)
	case sgreplicate.CONTINUOUS:
		return r.startContinuousReplication(parameters)
	default:
		return nil, errors.New("Unknown replication lifecycle")
	}
}

func (r *Replicator) stopReplication(repId string) error {
	replication := r.getReplication(repId)
	if replication == nil {
		return HTTPErrorf(http.StatusNotFound, "No replication found matching specified replication ID")
	}
	err := replication.Stop()
	if err != nil {
		return err
	}
	r.removeReplication(repId)
	return nil
}

func (r *Replicator) startOneShotReplication(parameters sgreplicate.ReplicationParameters) (sgreplicate.SGReplication, error) {

	replication := sgreplicate.StartOneShotReplication(parameters)
	r.addReplication(replication)

	if parameters.Async {
		go r.runOneShotReplication(replication)
		return replication, nil
	} else {
		err := r.runOneShotReplication(replication)
		return replication, err

	}
}

// Calls WaitUntilDone to work the notification channel for the one-shot replication.  Used for both synchronous and async one-shot replications.
func (r *Replicator) runOneShotReplication(replication *sgreplicate.Replication) error {
	defer r.removeReplication(replication.GetParameters().ReplicationId)
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
	r.addReplication(replication)
	LogTo("Replicate", "Started continuous replication: %v", replication)

	// Start goroutine to monitor notification channel, to remove the replication if it's terminated internally by sg-replicate
	go func(rep sgreplicate.SGReplication, notificationChan chan sgreplicate.ContinuousReplicationNotification) {
		defer r.removeReplication(rep.GetParameters().ReplicationId)
		for {
			select {
			case notification, ok := <-notificationChan:
				if !ok {
					LogTo("Replicate", "Replication %s was terminated.", rep.GetParameters().ReplicationId)
					return
				}
				LogTo("Replicate+", "Got notification %v", notification)
			}
		}
	}(replication, notificationChan)

	return replication, nil
}

func populateActiveTaskFromReplication (replication sgreplicate.SGReplication) (task *ActiveTask) {
	params := replication.GetParameters()
	stats := replication.GetStats()
	task = &ActiveTask{
		TaskType:         "replication",
		ReplicationID:    params.ReplicationId,
		Source:           params.GetSourceDbUrl(),
		Target:           params.GetTargetDbUrl(),
		Continuous:       params.Lifecycle == sgreplicate.CONTINUOUS,
		DocsRead:         stats.DocsRead,
		DocsWritten:      stats.DocsWritten,
		DocWriteFailures: stats.DocWriteFailures,
		StartLastSeq:     stats.StartLastSeq,
		EndLastSeq:       stats.EndLastSeq,
	}

	return
}