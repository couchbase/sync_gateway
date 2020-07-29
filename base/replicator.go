package base

import (
	"errors"
	"expvar"
	"net/http"
	"sync"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

const (
	defaultContinuousRetryTime = 500 * time.Millisecond
)

// Replication manager
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
	DocsRead         int64       `json:"docs_read"`
	DocsWritten      int64       `json:"docs_written"`
	DocWriteFailures int64       `json:"doc_write_failures"`
	StartLastSeq     int64       `json:"start_last_seq"`
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
		if task != nil {
			tasks = append(tasks, *task)
		}
	}

	return tasks
}

// StopReplications stops all active replications.
func (r *Replicator) StopReplications() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	for id, rep := range r.replications {
		Infof(KeyReplicate, "Stopping replication %s", UD(id))
		if err := rep.Stop(); err != nil {
			Warnf("Error stopping replication %s.  It's possible that the replication was already stopped and this can be safely ignored. Error: %v.", id, err)
		}
		Infof(KeyReplicate, "Stopped replication %s", UD(id))
	}

	r.replications = make(map[string]sgreplicate.SGReplication)
	r.replicationParams = make(map[string]sgreplicate.ReplicationParameters)

	return nil
}

// Starts a replication based on the provided replication config.
func (r *Replicator) startReplication(parameters sgreplicate.ReplicationParameters) (*Task, error) {
	// Generate ID if blank for the new replication
	if parameters.ReplicationId == "" {
		parameters.ReplicationId = GenerateRandomID()
	}

	parameters.LogFn = sgreplicateLogFn

	Infof(KeyReplicate, "Creating replication with parameters %s", UD(parameters))

	var (
		replication sgreplicate.SGReplication
		err         error
	)

	parameters.Stats = ReplicationStats(parameters.ReplicationId)

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

// ReplicationStats returns replication stats for the given replication ID, or a new set if they do not already exist.
func ReplicationStats(replicationID string) (stats *sgreplicate.ReplicationStats) {
	stats = sgreplicate.NewReplicationStats()
	if existingStats := SyncGatewayStats.ReplicationStats().Get(replicationID); existingStats != nil {
		existingStatsMap := existingStats.(*expvar.Map)
		stats.Active = existingStatsMap.Get(StatKeySgrActive).(*sgreplicate.AtomicBool)
		stats.DocsWritten = existingStatsMap.Get(StatKeySgrNumDocsPushed).(*expvar.Int)
		stats.DocWriteFailures = existingStatsMap.Get(StatKeySgrNumDocsFailedToPush).(*expvar.Int)
		stats.NumAttachmentsTransferred = existingStatsMap.Get(StatKeySgrNumAttachmentsTransferred).(*expvar.Int)
		stats.AttachmentBytesTransferred = existingStatsMap.Get(StatKeySgrAttachmentBytesTransferred).(*expvar.Int)
		stats.DocsCheckedSent = existingStatsMap.Get(StatKeySgrDocsCheckedSent).(*expvar.Int)
	} else {
		// Initialize replication stats
		SyncGatewayStats.ReplicationStats().Set(replicationID, ReplicationStatsMap(stats))
	}

	return
}

// ReplicationStatsMap returns an expvar.Map that contains references to stats in the given ReplicationStats
func ReplicationStatsMap(s *sgreplicate.ReplicationStats) *expvar.Map {
	m := expvar.Map{}
	m.Set(StatKeySgrActive, s.Active)
	m.Set(StatKeySgrNumDocsPushed, s.DocsWritten)
	m.Set(StatKeySgrNumDocsFailedToPush, s.DocWriteFailures)
	m.Set(StatKeySgrNumAttachmentsTransferred, s.NumAttachmentsTransferred)
	m.Set(StatKeySgrAttachmentBytesTransferred, s.AttachmentBytesTransferred)
	m.Set(StatKeySgrDocsCheckedSent, s.DocsCheckedSent)
	return &m
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

	Infof(KeyReplicate, "Started one-shot replication: %v", UD(replication))

	if parameters.Async {
		go func() {
			defer r.removeReplication(parameters.ReplicationId)
			if _, err := replication.WaitUntilDone(); err != nil {
				Warnf("async one-shot replication %s failed: %v", UD(parameters.ReplicationId), err)
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
		stats := &sgreplicate.ReplicationStats{
			DocsRead:                   parameters.Stats.DocsRead,
			DocsWritten:                parameters.Stats.DocsWritten,
			DocWriteFailures:           parameters.Stats.DocWriteFailures,
			StartLastSeq:               parameters.Stats.StartLastSeq,
			NumAttachmentsTransferred:  parameters.Stats.NumAttachmentsTransferred,
			AttachmentBytesTransferred: parameters.Stats.AttachmentBytesTransferred,
			DocsCheckedSent:            parameters.Stats.DocsCheckedSent,
			EndLastSeq:                 parameters.Stats.EndLastSeq,
			// Set 'Active' to a new AtomicBool for the child oneshot replication so the parent's is unaffected
			Active: &sgreplicate.AtomicBool{},
		}
		parameters.Stats = stats
		return sgreplicate.NewReplication(parameters, notificationChan)
	}

	replication := sgreplicate.NewContinuousReplication(parameters, factory, notificationChan, defaultContinuousRetryTime)
	r._addReplication(replication, parameters)
	r.lock.Unlock()

	Infof(KeyReplicate, "Started continuous replication: %v", UD(replication))

	// Start goroutine to monitor notification channel, to remove the replication if it's terminated internally by sg-replicate
	go func(rep sgreplicate.SGReplication, notificationChan chan sgreplicate.ContinuousReplicationNotification) {
		defer r.removeReplication(parameters.ReplicationId)

		for {
			select {
			case notification, ok := <-notificationChan:
				if !ok {
					Infof(KeyReplicate, "Replication %s was terminated.", UD(parameters.ReplicationId))
					return
				}
				Debugf(KeyReplicate, "Got notification %v", notification)
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

// removeReplication removes the given replication from the replicator maps.
func (r *Replicator) removeReplication(repID string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.replications, repID)
	delete(r.replicationParams, repID)
}

// _addReplication adds the given replication to the replicator maps.
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

	// inactive replications used to be removed - now we'll just exclude them from tasks
	if !stats.Active.Get() {
		return nil
	}

	return &Task{
		TaskType:         "replication",
		ReplicationID:    params.ReplicationId,
		Source:           params.GetSourceDbUrl(),
		Target:           params.GetTargetDbUrl(),
		Continuous:       params.Lifecycle == sgreplicate.CONTINUOUS,
		DocsRead:         stats.DocsRead.Value(),
		DocsWritten:      stats.DocsWritten.Value(),
		DocWriteFailures: stats.DocWriteFailures.Value(),
		StartLastSeq:     stats.StartLastSeq.Value(),
		EndLastSeq:       stats.EndLastSeq.Value(),
	}
}
