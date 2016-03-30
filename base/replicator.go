package base

import (
	"errors"
	"fmt"
	"sync"

	"github.com/couchbaselabs/sg-replicate"
)

type Replicator struct {
	replications map[string]*Replication
	lock         sync.RWMutex
}

// Replication defines an sg-replicate based replication being managed by this Sync Gateway instance.
type Replication struct {
	replicationId string
}

func (r *Replicator) Replicate(params sgreplicate.ReplicationParameters, isCancel bool) {

	if isCancel {
		r.stopReplication(params.ReplicationId)
	} else {
		r.startReplication(params)
	}

}

func (r *Replicator) addReplication(rep *Replication) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.replications[rep.replicationId] = rep
}

func (r *Replicator) getReplication(repId string) *Replication {
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
	defer r.lock.RUnlock()
	delete(r.replications, repId)
}

// Starts a replication based on the provided replication config.
func (r *Replicator) startReplication(parameters sgreplicate.ReplicationParameters) error {

	LogTo("Replicate", "Starting replication with parameters %+v", parameters)
	// Generate ID for the new replication

	switch parameters.Lifecycle {
	case sgreplicate.ONE_SHOT:
		_, err := sgreplicate.RunOneShotReplication(parameters)
		return err
	case sgreplicate.CONTINUOUS:
		return errors.New("Continuous replication not implemented")
	default:
		return errors.New("Unknown replication lifecycle")
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

func (rep *Replication) Stop() error {
	return nil
}
