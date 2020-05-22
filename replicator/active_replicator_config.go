package replicator

import (
	"fmt"
	"net/url"
)

type ActiveReplicatorDirection uint8

const (
	activeReplicatorTypeUnset ActiveReplicatorDirection = iota
	ActiveReplicatorTypePushAndPull
	ActiveReplicatorTypePush
	ActiveReplicatorTypePull
	activeReplicatorTypeCount
)

type ActiveReplicatorConfig struct {
	ID string
	// Type of replication: PushAndPull, Push, or Pull
	Direction ActiveReplicatorDirection
	// // CheckpointInterval controls many revisions to process before storing a checkpoint
	// CheckpointInterval uint16 // Default: 200
	// // ChangesBatchSize controls how many revisions may be batched per changes message
	// ChangesBatchSize uint16 // Default: 200
	// TargetDB represents the full Sync Gateway URL, including database path, and basic auth credentials
	TargetDB *url.URL
}

// Validate returns a slice of validation errors for the given replicator config.
// TODO: CBG-878 multierror util for this and other []error usages.
func (arc *ActiveReplicatorConfig) Validate() (errors []error) {
	if arc.Direction == activeReplicatorTypeUnset {
		errors = append(errors, fmt.Errorf("unset ActiveReplicatorDirection"))
	} else if arc.Direction < activeReplicatorTypeUnset || arc.Direction >= activeReplicatorTypeCount {
		errors = append(errors, fmt.Errorf("unknown ActiveReplicatorDirection value: %v", arc.Direction))
	}

	// if arc.ChangesBatchSize == 0 {
	// 	errors = append(errors, fmt.Errorf("unset ChangesBatchSize"))
	// }

	// if arc.CheckpointInterval == 0 {
	// 	errors = append(errors, fmt.Errorf("unset CheckpointInterval"))
	// }

	if arc.TargetDB == nil {
		errors = append(errors, fmt.Errorf("empty TargetDB URL"))
	} else {
		if arc.TargetDB.Host == "" {
			errors = append(errors, fmt.Errorf("empty host for TargetDB URL: %v", arc.TargetDB))
		}
		if arc.TargetDB.Path == "" {
			errors = append(errors, fmt.Errorf("empty database path for TargetDB URL: %v", arc.TargetDB))
		}
		if arc.TargetDB.Scheme != "http" && arc.TargetDB.Scheme != "https" {
			errors = append(errors, fmt.Errorf("unknown protocol scheme for TargetDB URL: %v", arc.TargetDB))
		}
	}

	return errors
}
