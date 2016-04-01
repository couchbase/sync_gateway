package base

import (
	"errors"
	"fmt"
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

func NewReplicator() *Replicator {
	return &Replicator{
		replications: make(map[string]sgreplicate.SGReplication),
	}
}

func (r *Replicator) Replicate(params sgreplicate.ReplicationParameters, isCancel bool) error {

	if isCancel {
		return r.stopReplication(params.ReplicationId)
	} else {
		_, err := r.startReplication(params)
		return err
	}

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
		return fmt.Errorf("No active replication with id %s", repId)
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
		return replication, nil
	}

	// If synchronous, blocks until complete
	defer r.removeReplication(parameters.ReplicationId)
	_, err := replication.WaitUntilDone()
	return nil, err
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
