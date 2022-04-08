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
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
)

const (
	cfgKeySGRCluster        = "sgrCluster" // key used for sgrCluster information in a cbgt.Cfg-based key value store
	maxSGRClusterCasRetries = 100          // Maximum number of CAS retries when attempting to update the sgr cluster configuration
	sgrClusterMgrContextID  = "sgr-mgr-"   // logging context ID prefix for sgreplicate manager
	defaultChangesBatchSize = 200          // default changes batch size if replication batch_size is unset
)

var DefaultCheckpointInterval = time.Second * 5 // default value used for time-based checkpointing

const (
	ReplicationStateStopped      = "stopped"
	ReplicationStateRunning      = "running"
	ReplicationStateReconnecting = "reconnecting"
	ReplicationStateResetting    = "resetting"
	ReplicationStateError        = "error"
	ReplicationStateStarting     = "starting"
)

// Replication config validation error messages
const (
	ConfigErrorIDTooLong                        = "Replication ID must be less than 160 characters"
	ConfigErrorUnknownFilter                    = "Unknown replication filter; try sync_gateway/bychannel"
	ConfigErrorMissingQueryParams               = "Replication specifies sync_gateway/bychannel filter but is missing query_params"
	ConfigErrorMissingRemote                    = "Replication remote must be specified"
	ConfigErrorMissingDirection                 = "Replication direction must be specified"
	ConfigErrorDuplicateCredentials             = "Auth credentials can be specified using remote_username/remote_password config properties or remote URL, but not both"
	ConfigErrorConfigBasedAdhoc                 = "adhoc=true is invalid for replication in Sync Gateway configuration"
	ConfigErrorInvalidConflictResolutionTypeFmt = "Conflict resolution type is invalid, valid values are %s/%s/%s/%s"
	ConfigErrorInvalidDirectionFmt              = "Invalid replication direction %q, valid values are %s/%s/%s"
	ConfigErrorBadChannelsArray                 = "Bad channels array in query_params for sync_gateway/bychannel filter"
)

// ClusterUpdateFunc is callback signature used when updating the cluster configuration
type ClusterUpdateFunc func(cluster *SGRCluster) (cancel bool, err error)

// SGRCluster defines sg-replicate configuration and distribution for a collection of Sync Gateway nodes
type SGRCluster struct {
	ClusterUUID  string                     `json:"cluster_uuid"` // A generated unique ID to identify the cluster
	Replications map[string]*ReplicationCfg `json:"replications"` // Set of replications defined for the cluster, indexed by replicationID
	Nodes        map[string]*SGNode         `json:"nodes"`        // Set of nodes, indexed by host name
	loggingCtx   context.Context            // logging context for cluster operations
}

func NewSGRCluster() *SGRCluster {
	return &SGRCluster{
		Replications: make(map[string]*ReplicationCfg),
		Nodes:        make(map[string]*SGNode),
	}
}

// SGNode represents a single Sync Gateway node in the cluster
type SGNode struct {
	UUID string `json:"uuid"` // Node UUID
	Host string `json:"host"` // Host name
}

func NewSGNode(uuid string, host string) *SGNode {
	return &SGNode{
		UUID: uuid,
		Host: host,
	}
}

// ReplicationConfig is a replication definition as stored in the Sync Gateway config
type ReplicationConfig struct {
	ID                     string                    `json:"replication_id"`
	Remote                 string                    `json:"remote"`
	Username               string                    `json:"username,omitempty"` // Deprecated
	Password               string                    `json:"password,omitempty"` // Deprecated
	RemoteUsername         string                    `json:"remote_username,omitempty"`
	RemotePassword         string                    `json:"remote_password,omitempty"`
	Direction              ActiveReplicatorDirection `json:"direction"`
	ConflictResolutionType ConflictResolverType      `json:"conflict_resolution_type,omitempty"`
	ConflictResolutionFn   string                    `json:"custom_conflict_resolver,omitempty"`
	PurgeOnRemoval         bool                      `json:"purge_on_removal,omitempty"`
	DeltaSyncEnabled       bool                      `json:"enable_delta_sync,omitempty"`
	MaxBackoff             int                       `json:"max_backoff_time,omitempty"`
	InitialState           string                    `json:"initial_state,omitempty"`
	Continuous             bool                      `json:"continuous"`
	Filter                 string                    `json:"filter,omitempty"`
	QueryParams            interface{}               `json:"query_params,omitempty"`
	Adhoc                  bool                      `json:"adhoc,omitempty"`
	BatchSize              int                       `json:"batch_size,omitempty"`
	RunAs                  string                    `json:"run_as,omitempty"`
}

func DefaultReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		DeltaSyncEnabled:       false,
		PurgeOnRemoval:         false,
		MaxBackoff:             5,
		ConflictResolutionType: ConflictResolverDefault,
		InitialState:           ReplicationStateRunning,
		Continuous:             false,
		Adhoc:                  false,
		BatchSize:              defaultChangesBatchSize,
	}
}

// ReplicationCfg represents a replication definition as stored in the cluster config.
type ReplicationCfg struct {
	ReplicationConfig
	ClusterUUID  string `json:"cluster_uuid,omitempty"` // UUID of the cluster for the replication, if it was defined at replication creation time
	AssignedNode string `json:"assigned_node"`          // UUID of node assigned to this replication
	TargetState  string `json:"target_state,omitempty"` // Target state for replication.
}

// ReplicationUpsertConfig is used for operations that support upsert of a subset of replication properties.
type ReplicationUpsertConfig struct {
	ID                     string      `json:"replication_id"`
	Remote                 *string     `json:"remote"`
	Username               *string     `json:"username,omitempty"` // Deprecated
	Password               *string     `json:"password,omitempty"` // Deprecated
	RemoteUsername         *string     `json:"remote_username,omitempty"`
	RemotePassword         *string     `json:"remote_password,omitempty"`
	Direction              *string     `json:"direction"`
	ConflictResolutionType *string     `json:"conflict_resolution_type,omitempty"`
	ConflictResolutionFn   *string     `json:"custom_conflict_resolver,omitempty"`
	PurgeOnRemoval         *bool       `json:"purge_on_removal,omitempty"`
	DeltaSyncEnabled       *bool       `json:"enable_delta_sync,omitempty"`
	MaxBackoff             *int        `json:"max_backoff_time,omitempty"`
	InitialState           *string     `json:"initial_state,omitempty"`
	Continuous             *bool       `json:"continuous"`
	Filter                 *string     `json:"filter,omitempty"`
	QueryParams            interface{} `json:"query_params,omitempty"`
	Adhoc                  *bool       `json:"adhoc,omitempty"`
	BatchSize              *int        `json:"batch_size,omitempty"`
	RunAs                  *string     `json:"run_as,omitempty"`
}

func (rc *ReplicationConfig) ValidateReplication(fromConfig bool) (err error) {

	// Perform EE checks first, to avoid error messages related to EE functionality
	if !base.IsEnterpriseEdition() {
		if rc.ConflictResolutionType != "" && rc.ConflictResolutionType != ConflictResolverDefault {
			return base.HTTPErrorf(http.StatusBadRequest, "Non-default conflict_resolution_type is only supported in enterprise edition")
		}
		if rc.DeltaSyncEnabled == true {
			return base.HTTPErrorf(http.StatusBadRequest, "Delta sync for sg-replicate is only supported in enterprise edition")
		}
		if rc.BatchSize > 0 && rc.BatchSize != defaultChangesBatchSize {
			return base.HTTPErrorf(http.StatusBadRequest, "Replication batch_size can only be configured in enterprise edition")
		}
	}

	// Checkpoint keys are prefixed with 35 characters:  _sync:local:checkpoint/sgr2cp:pull:
	// Setting length limit for replication ID to 160 characters to retain some room for future
	// key-related enhancements
	if len(rc.ID) > 160 {
		return base.HTTPErrorf(http.StatusBadRequest, ConfigErrorIDTooLong)
	}

	if rc.Adhoc {
		// Adhoc is only supported via the REST API
		if fromConfig {
			return base.HTTPErrorf(http.StatusBadRequest, ConfigErrorConfigBasedAdhoc)
		}

		if rc.InitialState == ReplicationStateStopped {
			return base.HTTPErrorf(http.StatusBadRequest, "Setting initial_state=stopped is not valid for replications specifying adhoc=true")
		}
	}

	if rc.Remote == "" {
		return base.HTTPErrorf(http.StatusBadRequest, ConfigErrorMissingRemote)
	}

	if !rc.ConflictResolutionType.IsValid() && rc.ConflictResolutionType != "" {
		return base.HTTPErrorf(http.StatusBadRequest, ConfigErrorInvalidConflictResolutionTypeFmt,
			ConflictResolverLocalWins, ConflictResolverRemoteWins, ConflictResolverDefault, ConflictResolverCustom)
	}

	if rc.ConflictResolutionType == ConflictResolverCustom && rc.ConflictResolutionFn == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Custom conflict resolution type has been set but no conflict resolution function has been defined")
	}

	remoteURL, err := url.Parse(rc.Remote)
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Replication remote URL is invalid")
	}

	if rc.RemoteUsername != "" && rc.Username != "" {
		return base.HTTPErrorf(http.StatusBadRequest,
			"Cannot set both remote_username and username config options. Please only use the remote_username and remote_password config options")
	}

	if (remoteURL != nil && remoteURL.User.Username() != "") && (rc.RemoteUsername != "" || rc.Username != "") {
		return base.HTTPErrorf(http.StatusBadRequest,
			ConfigErrorDuplicateCredentials)
	}

	if rc.Direction == "" {
		return base.HTTPErrorf(http.StatusBadRequest, ConfigErrorMissingDirection)
	}

	if !rc.Direction.IsValid() {
		return base.HTTPErrorf(http.StatusBadRequest, ConfigErrorInvalidDirectionFmt,
			rc.Direction, ActiveReplicatorTypePush, ActiveReplicatorTypePull, ActiveReplicatorTypePushAndPull)
	}

	if rc.Filter == base.ByChannelFilter {
		if rc.QueryParams == nil {
			return base.HTTPErrorf(http.StatusBadRequest, ConfigErrorMissingQueryParams)
		}

		_, invalidChannelsErr := ChannelsFromQueryParams(rc.QueryParams)
		if invalidChannelsErr != nil {
			return invalidChannelsErr
		}

	} else if rc.Filter != "" {
		return base.HTTPErrorf(http.StatusBadRequest, ConfigErrorUnknownFilter)
	}
	return nil
}

// Upsert updates ReplicationConfig with any non-empty properties specified in the incoming replication config.
// Note that if the intention is to reset the value to default, empty values must be specified.
func (rc *ReplicationConfig) Upsert(c *ReplicationUpsertConfig) {

	if c.Remote != nil {
		rc.Remote = *c.Remote
	}

	if c.Username != nil {
		rc.Username = *c.Username
	}

	if c.Password != nil {
		rc.Password = *c.Password
	}

	if c.RemoteUsername != nil {
		rc.RemoteUsername = *c.RemoteUsername
	}

	if c.RemotePassword != nil {
		rc.RemotePassword = *c.RemotePassword
	}

	if c.Direction != nil {
		rc.Direction = ActiveReplicatorDirection(*c.Direction)
	}

	if c.ConflictResolutionType != nil {
		rc.ConflictResolutionType = ConflictResolverType(*c.ConflictResolutionType)
	}
	if c.ConflictResolutionFn != nil {
		rc.ConflictResolutionFn = *c.ConflictResolutionFn
	}
	if c.PurgeOnRemoval != nil {
		rc.PurgeOnRemoval = *c.PurgeOnRemoval
	}

	if c.DeltaSyncEnabled != nil {
		rc.DeltaSyncEnabled = *c.DeltaSyncEnabled
	}
	if c.MaxBackoff != nil {
		rc.MaxBackoff = *c.MaxBackoff
	}
	if c.InitialState != nil {
		rc.InitialState = *c.InitialState
	}
	if c.Continuous != nil {
		rc.Continuous = *c.Continuous
	}
	if c.Filter != nil {
		rc.Filter = *c.Filter
	}
	if c.Adhoc != nil {
		rc.Adhoc = *c.Adhoc
	}
	if c.QueryParams != nil {
		rc.QueryParams = c.QueryParams
	}

	if c.BatchSize != nil {
		rc.BatchSize = *c.BatchSize
	}

	if c.RunAs != nil {
		rc.RunAs = *c.RunAs
	}

	if c.QueryParams != nil {
		// QueryParams can be either []interface{} or map[string]interface{}, so requires type-specific copying
		// avoid later mutating c.QueryParams
		switch qp := c.QueryParams.(type) {
		case []interface{}:
			newParams := make([]interface{}, len(qp))
			copy(newParams, qp)
			rc.QueryParams = newParams
		case map[string]interface{}:
			newParamMap := make(map[string]interface{})
			for k, v := range qp {
				newParamMap[k] = v
			}
			rc.QueryParams = newParamMap
		default:
			// unsupported query params type, don't upsert
			base.WarnfCtx(context.Background(), "Unexpected QueryParams type found during upsert, will be ignored (%T): %v", c.QueryParams, c.QueryParams)
		}
	}
}

// Equals is doing a relatively expensive json-based equality computation, so shouldn't be used in performance-sensitive scenarios
func (rc *ReplicationConfig) Equals(compareToCfg *ReplicationConfig) (bool, error) {
	currentBytes, err := base.JSONMarshalCanonical(rc)
	if err != nil {
		return false, err
	}
	compareToBytes, err := base.JSONMarshalCanonical(compareToCfg)
	if err != nil {
		return false, err
	}
	return bytes.Equal(currentBytes, compareToBytes), nil
}

// Redacted returns the ReplicationCfg with password of the remote database redacted from
// both replication config and remote URL, i.e., any password will be replaced with xxxxx.
func (rc *ReplicationConfig) Redacted() *ReplicationConfig {
	config := *rc
	if config.Password != "" {
		config.Password = base.RedactedStr
	}
	if config.RemotePassword != "" {
		config.RemotePassword = base.RedactedStr
	}
	config.Remote = base.RedactBasicAuthURLPassword(config.Remote)
	return &config
}

// sgReplicateManager should be used for all interactions with the stored cluster definition.
type sgReplicateManager struct {
	cfg                        cbgt.Cfg                      // Key-value store implementation
	loggingCtx                 context.Context               // logging context for manager operations
	heartbeatListener          *ReplicationHeartbeatListener // node heartbeat listener for replication distribution
	localNodeUUID              string                        // nodeUUID for this SG node
	activeReplicators          map[string]*ActiveReplicator  // currently assigned replications
	activeReplicatorsLock      sync.RWMutex                  // Mutex for activeReplications
	clusterUpdateTerminator    chan struct{}                 // Terminator for cluster update retry
	clusterSubscribeTerminator chan struct{}                 // Terminator for cluster change monitoring
	closeWg                    sync.WaitGroup                // Teardown waitgroup for subscribe and retry goroutines
	dbContext                  *DatabaseContext              // reference to the parent DatabaseContext
	CheckpointInterval         time.Duration                 // The value to be used for time-based checkpoints
}

// alignState attempts to update the current replicator state to align with the provided targetState, if
// it's a valid state transition.
//  Valid:
//     stopped -> running
//     stopped -> resetting
//     resetting -> stopped
//     running -> stopped
func (ar *ActiveReplicator) alignState(targetState string) error {
	if ar == nil {
		return nil
	}

	currentState, _ := ar.State()
	if targetState == currentState {
		return nil
	}

	switch targetState {
	case ReplicationStateStopped:
		base.InfofCtx(context.TODO(), base.KeyReplicate, "Stopping replication %s - previous state %s", ar.ID, currentState)
		stopErr := ar.Stop()
		if stopErr != nil {
			return fmt.Errorf("Unable to gracefully stop active replicator for replication %s: %v", ar.ID, stopErr)
		}
	case ReplicationStateRunning:
		if currentState == ReplicationStateReconnecting {
			// a replicator in a reconnecting state is already running
			return nil
		}
		base.InfofCtx(context.TODO(), base.KeyReplicate, "Starting replication %s - previous state %s", ar.ID, currentState)
		startErr := ar.Start()
		if startErr != nil {
			return fmt.Errorf("Unable to start active replicator for replication %s: %v", ar.ID, startErr)
		}
	case ReplicationStateResetting:
		if currentState != ReplicationStateStopped {
			return fmt.Errorf("Replication must be stopped before it can be reset")
		}
		base.InfofCtx(context.TODO(), base.KeyReplicate, "Resetting replication %s - previous state %s", ar.ID, currentState)
		resetErr := ar.Reset()
		if resetErr != nil {
			return fmt.Errorf("Unable to reset active replicator for replication %s: %v", ar.ID, resetErr)
		}
	}
	return nil

}

func (dbc *DatabaseContext) StartReplications() {
	if dbc.Options.SGReplicateOptions.Enabled {
		base.DebugfCtx(dbc.SGReplicateMgr.loggingCtx, base.KeyReplicate, "Will start Inter-Sync Gateway Replications for database %q", dbc.Name)
		dbc.SGReplicateMgr.closeWg.Add(1)
		go func() {
			defer dbc.SGReplicateMgr.closeWg.Done()

			// Wait for the server context to be started
			t := time.NewTimer(time.Second * 10)
			defer t.Stop()
			select {
			case <-dbc.ServerContextHasStarted:
				base.DebugfCtx(dbc.SGReplicateMgr.loggingCtx, base.KeyReplicate, "Server context started, starting ISGR replications %q", dbc.Name)
			case <-t.C:
				base.InfofCtx(dbc.SGReplicateMgr.loggingCtx, base.KeyReplicate, "Timed out waiting for server context startup... starting ISGR replications for %q anyway", dbc.Name)
			case <-dbc.terminator:
				base.DebugfCtx(dbc.SGReplicateMgr.loggingCtx, base.KeyReplicate, "Database context for %q closed before starting ISGR replications - aborting...", dbc.Name)
				return
			}

			err := dbc.SGReplicateMgr.StartReplications()
			if err != nil {
				base.ErrorfCtx(dbc.SGReplicateMgr.loggingCtx, "Error starting %q Inter-Sync Gateway Replications: %v", dbc.Name, err)
			}
		}()
	} else {
		base.DebugfCtx(context.TODO(), base.KeyReplicate, "Not starting Inter-Sync Gateway Replications for database %q - is disabled", dbc.Name)
	}
}

func NewSGReplicateManager(dbContext *DatabaseContext, cfg cbgt.Cfg) (*sgReplicateManager, error) {
	if cfg == nil {
		return nil, errors.New("Cfg must be provided for SGReplicateManager")
	}

	return &sgReplicateManager{
		cfg: cfg,
		loggingCtx: context.WithValue(context.Background(), base.LogContextKey{},
			base.LogContext{CorrelationID: sgrClusterMgrContextID + dbContext.Name}),
		clusterUpdateTerminator:    make(chan struct{}),
		clusterSubscribeTerminator: make(chan struct{}),
		dbContext:                  dbContext,
		activeReplicators:          make(map[string]*ActiveReplicator),
		CheckpointInterval:         DefaultCheckpointInterval,
	}, nil

}

// StartLocalNode registers the local node into the replication config, and registers an
// sg-replicate heartbeat listener with the provided heartbeater
func (m *sgReplicateManager) StartLocalNode(nodeUUID string, heartbeater base.Heartbeater) error {

	err := m.RegisterNode(nodeUUID)
	if err != nil {
		return err
	}

	m.localNodeUUID = nodeUUID

	// Heartbeat registration is EE-only, only register if heartbeater is non-nil
	if heartbeater == nil {
		return nil
	}

	m.heartbeatListener, err = NewReplicationHeartbeatListener(m)
	if err != nil {
		return err
	}

	base.InfofCtx(m.loggingCtx, base.KeyCluster, "Started local node for sg-replicate as %s", nodeUUID)
	return heartbeater.RegisterListener(m.heartbeatListener)
}

// StartReplications performs an initial retrieval of the cluster config, starts any replications
// assigned to this node, and starts the process to monitor future changes to the cluster config.
func (m *sgReplicateManager) StartReplications() error {

	replications, err := m.GetReplications()
	if err != nil {
		return err
	}
	for replicationID, replicationCfg := range replications {
		base.DebugfCtx(m.loggingCtx, base.KeyCluster, "Replication %s is assigned to node %s (local node is %s) on start up", replicationID, replicationCfg.AssignedNode, m.localNodeUUID)
		if replicationCfg.AssignedNode == m.localNodeUUID {
			activeReplicator, err := m.InitializeReplication(replicationCfg)
			if err != nil {
				base.WarnfCtx(m.loggingCtx, "Error initializing replication %s: %v", err)
				continue
			}
			m.activeReplicatorsLock.Lock()
			m.activeReplicators[replicationID] = activeReplicator
			m.activeReplicatorsLock.Unlock()
			if replicationCfg.TargetState == "" || replicationCfg.TargetState == ReplicationStateRunning {
				if startErr := activeReplicator.Start(); startErr != nil {
					base.WarnfCtx(m.loggingCtx, "Unable to start replication %s: %v", replicationID, startErr)
				}
			}
		}
	}
	return m.SubscribeCfgChanges()
}

// NewActiveReplicatorConfig converts an incoming ReplicationCfg to an ActiveReplicatorConfig
func (m *sgReplicateManager) NewActiveReplicatorConfig(config *ReplicationCfg) (rc *ActiveReplicatorConfig, err error) {
	insecureSkipVerify := false
	if m.dbContext.Options.UnsupportedOptions != nil {
		insecureSkipVerify = m.dbContext.Options.UnsupportedOptions.SgrTlsSkipVerify
	}

	activeDB := &Database{DatabaseContext: m.dbContext}
	if config.RunAs != "" {
		user, err := m.dbContext.Authenticator(m.loggingCtx).GetUser(config.RunAs)
		if err != nil {
			return nil, err
		}
		activeDB.SetUser(user)
	}

	rc = &ActiveReplicatorConfig{
		ID:                 config.ID,
		Continuous:         config.Continuous,
		ActiveDB:           activeDB,
		PurgeOnRemoval:     config.PurgeOnRemoval,
		DeltasEnabled:      config.DeltaSyncEnabled,
		InsecureSkipVerify: insecureSkipVerify,
		CheckpointInterval: m.CheckpointInterval,
		RunAs:              config.RunAs,
	}

	rc.MaxReconnectInterval = defaultMaxReconnectInterval
	if config.MaxBackoff != 0 {
		rc.MaxReconnectInterval = time.Duration(config.MaxBackoff) * time.Minute
	}

	// If maxBackoff is zero, retry up to ~MaxReconnectInterval and then give up.
	// If non-zero, reconnect is indefinite.
	if config.MaxBackoff == 0 {
		rc.TotalReconnectTimeout = rc.MaxReconnectInterval * 2
	}

	rc.ChangesBatchSize = defaultChangesBatchSize
	if config.BatchSize > 0 {
		rc.ChangesBatchSize = uint16(config.BatchSize)
	}

	// Channel filter processing
	if config.Filter == base.ByChannelFilter {
		rc.Filter = base.ByChannelFilter
		rc.FilterChannels, err = ChannelsFromQueryParams(config.QueryParams)
		if err != nil {
			return nil, err
		}
	}
	rc.Direction = config.Direction

	// Set conflict resolver for pull replications
	if rc.Direction == ActiveReplicatorTypePull || rc.Direction == ActiveReplicatorTypePushAndPull {
		if config.ConflictResolutionType == "" {
			rc.ConflictResolverFunc, err = NewConflictResolverFunc(ConflictResolverDefault, "")

		} else {
			rc.ConflictResolverFunc, err = NewConflictResolverFunc(config.ConflictResolutionType, config.ConflictResolutionFn)
			rc.ConflictResolverFuncSrc = config.ConflictResolutionFn
		}
		if err != nil {
			return nil, err
		}
		rc.ConflictResolutionType = config.ConflictResolutionType
	}

	if config.Remote == "" {
		return nil, fmt.Errorf("Replication remote must not be empty")
	}

	rc.RemoteDBURL, err = url.Parse(config.Remote)
	if err != nil {
		return nil, err
	}

	// If auth credentials are explicitly defined in the replication configuration,
	// enrich remote database URL connection string with the supplied auth credentials.
	username := config.Username
	password := config.Password
	if username != "" || password != "" {
		base.WarnfCtx(m.loggingCtx, `Deprecation notice: replication config fields "username" and "password" are now "remote_username" and "remote_password" respectively`)
	}
	if config.RemoteUsername != "" {
		username = config.RemoteUsername
		password = config.RemotePassword
	}
	if username != "" {
		rc.RemoteDBURL.User = url.UserPassword(username, password)
	}

	rc.WebsocketPingInterval = m.dbContext.Options.SGReplicateOptions.WebsocketPingInterval

	rc.onComplete = m.replicationComplete

	return rc, nil
}

func (m *sgReplicateManager) isCfgChanged(newCfg *ReplicationCfg, activeCfg *ActiveReplicatorConfig) (bool, error) {
	newConfig, err := m.NewActiveReplicatorConfig(newCfg)
	if err != nil {
		return true, err
	}
	return !newConfig.Equals(activeCfg), nil
}

func (m *sgReplicateManager) InitializeReplication(config *ReplicationCfg) (replicator *ActiveReplicator, err error) {

	base.InfofCtx(m.loggingCtx, base.KeyReplicate, "Initializing replication %v", base.UD(config.ID))

	// Re-validate replication.  Replication config should have already been validated on creation/update, but
	// re-checking here in case of issues during persistence/load.
	configValidationError := config.ValidateReplication(false)
	if configValidationError != nil {
		return nil, fmt.Errorf("Validation failure for replication config when initializing replication %s: %v", base.UD(config.ID), configValidationError)
	}

	rc, cfgErr := m.NewActiveReplicatorConfig(config)
	if cfgErr != nil {
		return nil, cfgErr
	}

	checkpointPrefix := ""
	// ClusterUUID is to prevent collisions between multiple remote replication checkpoints (e.g. two edge replications sharing the same ID)
	if config.ClusterUUID != "" {
		checkpointPrefix += config.ClusterUUID + ":"
	}
	// GroupID is to prevent collisions between multiple local replication checkpoints within different groups.
	if m.dbContext.Options.GroupID != "" {
		checkpointPrefix += m.dbContext.Options.GroupID + ":"
	}
	rc.checkpointPrefix = checkpointPrefix

	// Retrieve or create an entry in db.replications expvar for this replication
	allReplicationsStatsMap := m.dbContext.DbStats.DBReplicatorStats(rc.ID)
	rc.ReplicationStatsMap = allReplicationsStatsMap

	replicator = NewActiveReplicator(rc)

	return replicator, nil
}

// replicationComplete updates the replication status.
func (m *sgReplicateManager) replicationComplete(replicationID string) {
	err := m.UpdateReplicationState(replicationID, ReplicationStateStopped)
	if err != nil {
		base.WarnfCtx(m.loggingCtx, "Unable to update replication state to stopped on completion: %v", err)
	}
}

func (m *sgReplicateManager) Stop() {

	// Close subscribe terminator first to stop subscribing/responding to cluster config changes prior to
	// stopping replications and removing node
	close(m.clusterSubscribeTerminator)

	// Stop active replications
	m.activeReplicatorsLock.Lock()

	for _, repl := range m.activeReplicators {
		err := repl.Stop()
		if err != nil {
			base.WarnfCtx(m.loggingCtx, "Error stopping replication %s during manager stop: %v", repl.ID, err)
		}
	}
	m.activeReplicatorsLock.Unlock()
	if err := m.RemoveNode(m.localNodeUUID); err != nil {
		base.WarnfCtx(m.loggingCtx, "Attempt to remove node %v from sg-replicate cfg got error: %v", m.localNodeUUID, err)
	}
	if m.heartbeatListener != nil {
		m.heartbeatListener.Stop()
	}
	close(m.clusterUpdateTerminator)

	m.closeWg.Wait()
}

// RefreshReplicationCfg is called when the cfg changes.  Checks whether replications
// have been added to or removed from this node
func (m *sgReplicateManager) RefreshReplicationCfg() error {

	base.InfofCtx(m.loggingCtx, base.KeyCluster, "Replication definitions changed - refreshing...")
	configReplications, err := m.GetReplications()
	if err != nil {
		return err
	}

	m.activeReplicatorsLock.Lock()
	defer m.activeReplicatorsLock.Unlock()

	// check for active replications that should be stopped
	for replicationID, activeReplicator := range m.activeReplicators {
		replicationCfg, ok := configReplications[replicationID]

		// Check for active replications removed from this node
		if !ok || replicationCfg.AssignedNode != m.localNodeUUID {
			base.InfofCtx(m.loggingCtx, base.KeyReplicate, "Stopping reassigned replication %s", replicationID)
			err := activeReplicator.Stop()
			if err != nil {
				base.WarnfCtx(m.loggingCtx, "Unable to gracefully close active replication: %v", err)
			}
			if !ok {
				activeReplicator.purgeCheckpoints()
			}
			delete(m.activeReplicators, replicationID)

		} else {
			// Check for replications assigned to this node with updated state
			base.DebugfCtx(m.loggingCtx, base.KeyReplicate, "Aligning state for existing replication %s", replicationID)

			// If the config has changed, re-initialize the replication
			isChanged, err := m.isCfgChanged(replicationCfg, activeReplicator.config)
			if err != nil {
				base.WarnfCtx(m.loggingCtx, "Error evaluating whether cfg has changed, potential changes not applied: %v", err)
			}
			if isChanged {
				replicator, initError := m.InitializeReplication(replicationCfg)
				if initError != nil {
					base.WarnfCtx(m.loggingCtx, "Error initializing upserted replication %s: %v", initError)
				} else {
					m.activeReplicators[replicationID] = replicator
					activeReplicator = replicator
				}
			}

			stateErr := activeReplicator.alignState(replicationCfg.TargetState)
			if stateErr != nil {
				base.WarnfCtx(m.loggingCtx, "Error updating active replication %s to state %s: %v", replicationID, replicationCfg.TargetState, stateErr)
			}
			// Reset is synchronous - after completion the replication state should be updated to stopped
			if replicationCfg.TargetState == ReplicationStateResetting {
				postResetErr := m.UpdateReplicationState(replicationCfg.ID, ReplicationStateStopped)
				if postResetErr != nil {
					base.WarnfCtx(m.loggingCtx, "Error updating replication state to stopped after successful reset: %v", postResetErr)
				}

			}
		}
	}

	// Check for replications newly assigned to this node
	for replicationID, replicationCfg := range configReplications {
		if replicationCfg.AssignedNode == m.localNodeUUID {
			replicator, exists := m.activeReplicators[replicationID]
			if !exists {
				var initError error
				replicator, initError = m.InitializeReplication(replicationCfg)
				if initError != nil {
					base.WarnfCtx(m.loggingCtx, "Error initializing replication %s: %v", initError)
					continue
				}
				m.activeReplicators[replicationID] = replicator

				if replicationCfg.TargetState == "" || replicationCfg.TargetState == ReplicationStateRunning {
					base.InfofCtx(m.loggingCtx, base.KeyReplicate, "Starting newly assigned replication %s", replicationID)
					if startErr := replicator.Start(); startErr != nil {
						base.WarnfCtx(m.loggingCtx, "Unable to start replication after refresh %s: %v", replicationID, startErr)
					}
				}
			}
		}
	}
	return nil
}

func (m *sgReplicateManager) SubscribeCfgChanges() error {
	cfgEvents := make(chan cbgt.CfgEvent)

	err := m.cfg.Subscribe(cfgKeySGRCluster, cfgEvents)
	if err != nil {
		base.DebugfCtx(m.loggingCtx, base.KeyCluster, "Error subscribing to %s key changes: %v", cfgKeySGRCluster, err)
		return err
	}
	m.closeWg.Add(1)
	go func() {
		defer base.FatalPanicHandler()
		defer m.closeWg.Done()
		for {
			select {
			case _, ok := <-cfgEvents:
				if !ok {
					return
				}
				err := m.RefreshReplicationCfg()
				if err != nil {
					base.WarnfCtx(m.loggingCtx, "Error while updating replications based on latest cfg: %v", err)
				}
			case <-m.clusterSubscribeTerminator:
				return
			}
		}
	}()
	return nil
}

// Loads the SGReplicate config from the config store
func (m *sgReplicateManager) loadSGRCluster() (sgrCluster *SGRCluster, cas uint64, err error) {
	var cfgBytes []byte
	cfgBytes, cas, err = m.cfg.Get(cfgKeySGRCluster, 0)
	if err != nil {
		return nil, 0, err
	}

	if cfgBytes == nil {
		sgrCluster = NewSGRCluster()
		cas = 0
	} else {
		sgrCluster = &SGRCluster{}
		err := base.JSONUnmarshal(cfgBytes, sgrCluster)
		if err != nil {
			return nil, 0, err
		}
	}

	if sgrCluster.ClusterUUID == "" {
		sgrCluster.ClusterUUID = uuid.NewString()
	}

	sgrCluster.loggingCtx = m.loggingCtx
	return sgrCluster, cas, nil
}

// updateCluster manages CAS retry for SGRCluster updates.
func (m *sgReplicateManager) updateCluster(callback ClusterUpdateFunc) error {
	m.closeWg.Add(1)
	defer m.closeWg.Done()
	for i := 1; i <= maxSGRClusterCasRetries; i++ {
		select {
		case <-m.clusterUpdateTerminator:
			base.DebugfCtx(m.loggingCtx, base.KeyReplicate, "manager terminated, bailing out of update retry loop")
			return nil
		default:
		}

		sgrCluster, cas, err := m.loadSGRCluster()
		if err != nil {
			return err
		}
		cancel, callbackError := callback(sgrCluster)
		if callbackError != nil {
			return callbackError
		}
		if cancel {
			return nil
		}

		updatedBytes, err := base.JSONMarshal(sgrCluster)
		if err != nil {
			return err
		}

		_, err = m.cfg.Set(cfgKeySGRCluster, updatedBytes, cas)
		if err == base.ErrCfgCasError {
			base.DebugfCtx(m.loggingCtx, base.KeyReplicate, "CAS Retry updating sg-replicate cluster definition (%d/%d)", i, maxSGRClusterCasRetries)
		} else if err != nil {
			base.InfofCtx(m.loggingCtx, base.KeyReplicate, "Error persisting sg-replicate cluster definition - update cancelled: %v", err)
			return err
		} else {
			base.DebugfCtx(m.loggingCtx, base.KeyReplicate, "Successfully persisted sg-replicate cluster definition.")
			return nil
		}
	}
	return errors.New("CAS Retry count (%d) exceeded when attempting to update sg-replicate cluster configuration")
}

// Register node node adds a node to the cluster config with host=os.Hostname(), then triggers replication rebalance.
// Retries on CAS failure, is no-op if node already exists in config
func (m *sgReplicateManager) RegisterNode(nodeUUID string) error {

	hostName, hostErr := os.Hostname()
	if hostErr != nil {
		base.InfofCtx(m.loggingCtx, base.KeyCluster, "Unable to retrieve hostname, registering node for sgreplicate without host specified.  Error: %v", hostErr)
	}
	return m.registerNodeForHost(nodeUUID, hostName)
}

// registerNodeForHost node adds a node to the cluster config, then triggers replication rebalance.
// Retries on CAS failure, is no-op if node already exists in config.  Split out of RegisterNode to support
// testing with specified host values.
func (m *sgReplicateManager) registerNodeForHost(nodeUUID string, host string) error {
	registerNodeCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		_, exists := cluster.Nodes[nodeUUID]
		if exists {
			return true, nil
		}
		cluster.Nodes[nodeUUID] = NewSGNode(nodeUUID, host)
		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(registerNodeCallback)
}

// RemoveNode removes a node from the cluster config, then triggers replication rebalance.
// Retries on CAS failure, is no-op if node doesn't exist in config
func (m *sgReplicateManager) RemoveNode(nodeUUID string) error {
	removeNodeCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		_, exists := cluster.Nodes[nodeUUID]
		if !exists {
			return true, nil
		}
		delete(cluster.Nodes, nodeUUID)
		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(removeNodeCallback)
}

func (m *sgReplicateManager) getNodes() (nodes map[string]*SGNode, err error) {
	sgrCluster, _, err := m.loadSGRCluster()
	if err != nil {
		return nil, err
	}
	return sgrCluster.Nodes, nil
}

// GetSGRCluster returns SGReplicate configuration including nodes participating in
// replication distribution, per database.
func (m *sgReplicateManager) GetSGRCluster() (sgrCluster *SGRCluster, err error) {
	if sgrCluster, _, err = m.loadSGRCluster(); err != nil {
		return nil, err
	}
	return sgrCluster, nil
}

// GET _replication/{replicationID}
func (m *sgReplicateManager) GetReplication(replicationID string) (*ReplicationCfg, error) {
	sgrCluster, _, err := m.loadSGRCluster()
	if err != nil {
		return nil, err
	}
	replication, exists := sgrCluster.Replications[replicationID]
	if !exists {
		return nil, base.ErrNotFound
	}

	// TODO: remove the local/non-local handling below when CBG-909 is completed
	if replication.AssignedNode == m.localNodeUUID {
		replication.AssignedNode = replication.AssignedNode + " (local)"
	} else {
		replication.AssignedNode = replication.AssignedNode + " (non-local)"
	}

	return replication, nil
}

// GET _replication
func (m *sgReplicateManager) GetReplications() (replications map[string]*ReplicationCfg, err error) {

	sgrCluster, _, err := m.loadSGRCluster()
	if err != nil {
		return nil, err
	}
	return sgrCluster.Replications, nil
}

// PUT _replication/replicationID
func (m *sgReplicateManager) AddReplication(replication *ReplicationCfg) error {
	addReplicationCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		_, exists := cluster.Replications[replication.ID]
		if exists {
			return false, base.ErrAlreadyExists
		}
		if replication.ClusterUUID == "" {
			replication.ClusterUUID = cluster.ClusterUUID
		}
		cluster.Replications[replication.ID] = replication
		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(addReplicationCallback)
}

// PutReplications sets the value of one or more replications in the config
func (m *sgReplicateManager) PutReplications(replications map[string]*ReplicationConfig) error {
	addReplicationCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		if len(replications) == 0 {
			return true, nil
		}
		for replicationID, replication := range replications {
			existingCfg, exists := cluster.Replications[replicationID]
			replicationCfg := &ReplicationCfg{}
			if exists {
				replicationCfg.AssignedNode = existingCfg.AssignedNode
				replicationCfg.TargetState = existingCfg.TargetState
			} else {
				if replication.InitialState == ReplicationStateStopped {
					replicationCfg.TargetState = ReplicationStateStopped
				} else {
					replicationCfg.TargetState = ReplicationStateRunning
				}
				// New replication, so stamp ClusterUUID into the replication config
				if replicationCfg.ClusterUUID == "" {
					replicationCfg.ClusterUUID = cluster.ClusterUUID
				}
			}
			replicationCfg.ReplicationConfig = *replication
			cluster.Replications[replicationID] = replicationCfg
		}
		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(addReplicationCallback)
}

// PUT _replication/replicationID
func (m *sgReplicateManager) UpsertReplication(replication *ReplicationUpsertConfig) (created bool, err error) {

	created = true
	addReplicationCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		_, exists := cluster.Replications[replication.ID]
		if exists {
			created = false
			// If replication already exists ensure its in the stopped state before allowing upsert
			state, err := m.GetReplicationStatus(replication.ID, DefaultReplicationStatusOptions())
			if err != nil {
				return true, err
			}
			if state.Status != ReplicationStateStopped {
				return true, base.HTTPErrorf(http.StatusBadRequest, "Replication must be stopped before updating config")
			}
		} else {
			// Add a new replication to the cfg.  Set targetState based on initialState when specified.
			replicationConfig := DefaultReplicationConfig()
			replicationConfig.ID = replication.ID
			targetState := ReplicationStateRunning
			if replication.InitialState != nil && *replication.InitialState == ReplicationStateStopped {
				targetState = ReplicationStateStopped
			}
			cluster.Replications[replication.ID] = &ReplicationCfg{
				ReplicationConfig: replicationConfig,
				TargetState:       targetState,
			}
		}

		cluster.Replications[replication.ID].Upsert(replication)

		validateErr := cluster.Replications[replication.ID].ValidateReplication(false)
		if validateErr != nil {
			return true, validateErr
		}

		cluster.RebalanceReplications()
		return false, nil
	}
	return created, m.updateCluster(addReplicationCallback)
}

func (m *sgReplicateManager) UpdateReplicationState(replicationID string, state string) error {

	updateReplicationStatusCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		replicationCfg, exists := cluster.Replications[replicationID]
		if !exists {
			return true, base.ErrNotFound
		}

		stateChangeErr := isValidStateChange(replicationCfg.TargetState, state)
		if stateChangeErr != nil {
			return true, stateChangeErr
		}

		if state == ReplicationStateStopped && replicationCfg.Adhoc == true {
			delete(cluster.Replications, replicationID)
			cluster.RebalanceReplications()
			return false, nil
		}

		cluster.Replications[replicationID].TargetState = state
		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(updateReplicationStatusCallback)
}

func isValidStateChange(currentState, newState string) error {

	switch newState {
	case ReplicationStateRunning:
		if currentState == ReplicationStateResetting {
			return base.HTTPErrorf(http.StatusBadRequest, "Replication cannot be started until reset is complete")
		}
		return nil
	case ReplicationStateStopped:
		return nil
	case ReplicationStateResetting:
		if currentState == ReplicationStateRunning {
			return base.HTTPErrorf(http.StatusBadRequest, "Replication must be stopped before it can be reset")
		}
	}
	return nil
}

func transitionStateName(currentState, targetState string) string {
	transitionState := targetState

	switch targetState {
	case ReplicationStateRunning:
		if currentState != targetState {
			transitionState = "starting"
		}
	case ReplicationStateStopped:
		if currentState != targetState {
			transitionState = "stopping"
		}
	}

	return transitionState
}

// DELETE _replication
func (m *sgReplicateManager) DeleteReplication(replicationID string) error {
	deleteReplicationCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		_, exists := cluster.Replications[replicationID]
		if !exists {
			return false, base.ErrNotFound
		}
		delete(cluster.Replications, replicationID)
		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(deleteReplicationCallback)
}

func (c *SGRCluster) GetReplicationIDsForNode(nodeUUID string) (replicationIDs []string) {
	replicationIDs = make([]string, 0)
	for id, replication := range c.Replications {
		if replication.AssignedNode == nodeUUID {
			replicationIDs = append(replicationIDs, id)
		}
	}
	return replicationIDs
}

// RebalanceReplications distributes the set of defined replications across the set of available nodes
func (c *SGRCluster) RebalanceReplications() {

	base.DebugfCtx(c.loggingCtx, base.KeyReplicate, "Initiating replication rebalance.  Nodes: %d  Replications: %d", len(c.Nodes), len(c.Replications))
	defer base.DebugfCtx(c.loggingCtx, base.KeyReplicate, "Replication rebalance complete, persistence is pending...")

	// Identify unassigned replications that need to be distributed.
	// This includes both replications not assigned to a node, as well as replications
	// assigned to a node that is no longer part of the cluster.
	unassignedReplications := make(map[string]*ReplicationCfg)
	for replicationID, replication := range c.Replications {
		if replication.AssignedNode == "" {
			unassignedReplications[replicationID] = replication
		} else {
			// If replication has an assigned node, remove that assignment if node no longer exists in cluster
			_, ok := c.Nodes[replication.AssignedNode]
			if !ok {
				replication.AssignedNode = ""
				unassignedReplications[replicationID] = replication
				base.DebugfCtx(c.loggingCtx, base.KeyReplicate, "Replication %s unassigned, previous node no longer active.", replicationID)
			}
		}
	}

	// If there aren't any nodes available, there's nothing more to be done
	if len(c.Nodes) == 0 {
		return
	}

	// Construct a slice of sortableSGNodes, for sorting nodes by the number of assigned replications
	nodesByReplicationCount := make(NodesByReplicationCount, 0)
	for host := range c.Nodes {
		sortableNode := &sortableSGNode{
			host:                   host,
			assignedReplicationIDs: c.GetReplicationIDsForNode(host),
		}
		nodesByReplicationCount = append(nodesByReplicationCount, sortableNode)
	}
	sort.Sort(nodesByReplicationCount)

	// Assign unassigned replications to nodes with the fewest replications already assigned
	for replicationID, replication := range unassignedReplications {
		replication.AssignedNode = nodesByReplicationCount[0].host
		nodesByReplicationCount[0].assignedReplicationIDs = append(nodesByReplicationCount[0].assignedReplicationIDs, replicationID)
		base.DebugfCtx(c.loggingCtx, base.KeyReplicate, "Replication %s assigned to %s on update.", replicationID, nodesByReplicationCount[0].host)
		sort.Sort(nodesByReplicationCount)
	}

	// If there is more than one node, balance replications across nodes.
	// When the difference in the number of replications between any two nodes
	// is greater than 1, move a replication from the higher to lower count node.
	numNodes := len(nodesByReplicationCount)
	if len(nodesByReplicationCount) <= 1 {
		return
	}

	for {
		lowCountNode := nodesByReplicationCount[0]
		highCountNode := nodesByReplicationCount[numNodes-1]
		if len(highCountNode.assignedReplicationIDs)-len(lowCountNode.assignedReplicationIDs) < 2 {
			break
		}
		replicationToMove := highCountNode.assignedReplicationIDs[0]
		highCountNode.assignedReplicationIDs = highCountNode.assignedReplicationIDs[1:]
		base.DebugfCtx(c.loggingCtx, base.KeyReplicate, "Replication %s unassigned from %s.", replicationToMove, highCountNode.host)
		lowCountNode.assignedReplicationIDs = append(lowCountNode.assignedReplicationIDs, replicationToMove)
		c.Replications[replicationToMove].AssignedNode = lowCountNode.host
		base.DebugfCtx(c.loggingCtx, base.KeyReplicate, "Replication %s assigned to %s on update.", replicationToMove, lowCountNode.host)
		sort.Sort(nodesByReplicationCount)
	}

}

// sortableSGNode and NodesByReplicationCount are used to sort a set of nodes based on the number of replications
// assigned to each node.
type sortableSGNode struct {
	host                   string
	assignedReplicationIDs []string
}

type NodesByReplicationCount []*sortableSGNode

func (a NodesByReplicationCount) Len() int      { return len(a) }
func (a NodesByReplicationCount) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a NodesByReplicationCount) Less(i, j int) bool {
	return len(a[i].assignedReplicationIDs) < len(a[j].assignedReplicationIDs)
}

// ReplicationStatus is used by the _replicationStatus REST API endpoints
type ReplicationStatus struct {
	PullReplicationStatus
	PushReplicationStatus
	ID           string             `json:"replication_id"`
	Config       *ReplicationConfig `json:"config,omitempty"`
	Status       string             `json:"status"`
	ErrorMessage string             `json:"error_message,omitempty"`
}

type PullReplicationStatus struct {
	DocsRead        int64  `json:"docs_read,omitempty"`
	DocsCheckedPull int64  `json:"docs_checked_pull,omitempty"`
	DocsPurged      int64  `json:"docs_purged,omitempty"`
	RejectedLocal   int64  `json:"rejected_by_local,omitempty"`
	LastSeqPull     string `json:"last_seq_pull,omitempty"`
	DeltasRecv      int64  `json:"deltas_recv,omitempty"`
	DeltasRequested int64  `json:"deltas_requested,omitempty"`
}

type PushReplicationStatus struct {
	DocsWritten      int64  `json:"docs_written,omitempty"`
	DocsCheckedPush  int64  `json:"docs_checked_push,omitempty"`
	DocWriteFailures int64  `json:"doc_write_failures,omitempty"`
	DocWriteConflict int64  `json:"doc_write_conflict,omitempty"`
	RejectedRemote   int64  `json:"rejected_by_remote,omitempty"`
	LastSeqPush      string `json:"last_seq_push,omitempty"`
	DeltasSent       int64  `json:"deltas_sent,omitempty"`
}

// Add adds the value of all counter stats in other to ReplicationStatus
func (rs *PullReplicationStatus) Add(other PullReplicationStatus) {
	if rs == nil {
		return
	}
	rs.DocsRead += other.DocsRead
	rs.DocsCheckedPull += other.DocsCheckedPull
	rs.DocsPurged += other.DocsPurged
	rs.RejectedLocal += other.RejectedLocal
	rs.DeltasRecv += other.DeltasRecv
	rs.DeltasRequested += other.DeltasRequested
}

// Add adds the value of all counter stats in other to ReplicationStatus
func (rs *PushReplicationStatus) Add(other PushReplicationStatus) {
	if rs == nil {
		return
	}
	rs.DocsWritten += other.DocsWritten
	rs.DocsCheckedPush += other.DocsCheckedPush
	rs.DocWriteFailures += other.DocWriteFailures
	rs.DocWriteConflict += other.DocWriteConflict
	rs.RejectedRemote += other.RejectedRemote
	rs.DeltasSent += other.DeltasSent
}

type ReplicationStatusOptions struct {
	IncludeConfig bool // Include replication config
	LocalOnly     bool // Local replications only
	ActiveOnly    bool // Active replications only (
	IncludeError  bool // Exclude replication in error state
}

func DefaultReplicationStatusOptions() ReplicationStatusOptions {
	return ReplicationStatusOptions{
		IncludeConfig: false,
		LocalOnly:     false,
		ActiveOnly:    false,
		IncludeError:  true,
	}
}

func (m *sgReplicateManager) GetReplicationStatus(replicationID string, options ReplicationStatusOptions) (*ReplicationStatus, error) {

	// Check if replication is assigned locally
	m.activeReplicatorsLock.RLock()
	replication, isLocal := m.activeReplicators[replicationID]
	m.activeReplicatorsLock.RUnlock()

	if options.LocalOnly && !isLocal {
		return nil, nil
	}

	var status *ReplicationStatus
	var remoteCfg *ReplicationCfg
	if isLocal {
		status = replication.GetStatus()
	} else {
		// Attempt to retrieve persisted status
		var loadErr error
		status, loadErr = LoadReplicationStatus(m.dbContext, replicationID)
		if loadErr != nil {
			// Unable to load persisted status.  Create status stub based on config
			var err error
			remoteCfg, err = m.GetReplication(replicationID)
			if err != nil {
				return nil, err
			}
			status = &ReplicationStatus{
				ID:     replicationID,
				Status: remoteCfg.TargetState,
			}
		}
	}

	// Add the replication config if requested
	if options.IncludeConfig {
		if remoteCfg == nil {
			var err error
			remoteCfg, err = m.GetReplication(replicationID)
			if err != nil {
				return nil, err
			}
		}
		status.Config = remoteCfg.ReplicationConfig.Redacted()
	}

	if !options.IncludeError && status.Status == ReplicationStateError {
		return nil, nil
	}
	if options.ActiveOnly && status.Status != ReplicationStateRunning {
		return nil, nil
	}

	return status, nil
}

func (m *sgReplicateManager) PutReplicationStatus(replicationID, action string) (status *ReplicationStatus, err error) {

	targetState := ""
	switch action {
	case "reset":
		targetState = ReplicationStateResetting
	case "stop":
		targetState = ReplicationStateStopped
	case "start":
		targetState = ReplicationStateRunning
	default:
		return nil, base.HTTPErrorf(http.StatusBadRequest, "Unrecognized action %q.  Valid values are start/stop/reset.", action)
	}

	err = m.UpdateReplicationState(replicationID, targetState)
	if err != nil {
		return nil, err
	}

	updatedStatus, err := m.GetReplicationStatus(replicationID, DefaultReplicationStatusOptions())
	if err != nil {
		// Not found is expected when adhoc replication is stopped, return removed status instead of error
		// since UpdateReplicationState was successful
		if err == base.ErrNotFound {
			replicationStatus := &ReplicationStatus{
				ID:     replicationID,
				Status: "removed",
			}
			return replicationStatus, nil
		} else {
			return nil, err
		}
	}

	// Modify the returned replication state to align with the requested state
	updatedStatus.Status = transitionStateName(updatedStatus.Status, targetState)
	return updatedStatus, nil
}

func (m *sgReplicateManager) GetReplicationStatusAll(options ReplicationStatusOptions) ([]*ReplicationStatus, error) {

	statuses := make([]*ReplicationStatus, 0)

	// Include persisted replications
	persistedReplications, err := m.GetReplications()
	if err != nil {
		return nil, err
	}

	for replicationID, _ := range persistedReplications {
		status, err := m.GetReplicationStatus(replicationID, options)
		if err != nil {
			base.WarnfCtx(m.loggingCtx, "Unable to retrieve replication status for replication %s", replicationID)
		}
		if status != nil {
			statuses = append(statuses, status)
		}
	}

	return statuses, nil
}

// ImportHeartbeatListener uses replication cfg to manage node list
type ReplicationHeartbeatListener struct {
	mgr        *sgReplicateManager
	terminator chan struct{} // close manager subscription on close
	nodeIDs    []string      // Set of nodes from the latest retrieval
	lock       sync.RWMutex  // lock for nodeIDs access
}

func NewReplicationHeartbeatListener(mgr *sgReplicateManager) (*ReplicationHeartbeatListener, error) {

	if mgr == nil {
		return nil, errors.New("sgReplicateManager must not be nil for ReplicationHeartbeatListener")
	}

	listener := &ReplicationHeartbeatListener{
		mgr:        mgr,
		terminator: make(chan struct{}),
	}

	// Initialize the node set
	_, err := listener.reloadNodes()
	if err != nil {
		return nil, err
	}

	// Subscribe to changes to the known node set key
	err = listener.subscribeNodeSetChanges()
	if err != nil {
		return nil, err
	}

	return listener, nil
}

func (l *ReplicationHeartbeatListener) Name() string {
	return "sgReplicateListener"
}

// When we detect other nodes have stopped pushing heartbeats, use manager to remove from cfg
func (l *ReplicationHeartbeatListener) StaleHeartbeatDetected(nodeUUID string) {

	base.InfofCtx(l.mgr.loggingCtx, base.KeyCluster, "StaleHeartbeatDetected by sg-replicate listener for node: %v", nodeUUID)
	err := l.mgr.RemoveNode(nodeUUID)
	if err != nil {
		base.WarnfCtx(l.mgr.loggingCtx, "Attempt to remove node %v from sg-replicate cfg got error: %v", nodeUUID, err)
	}
}

// subscribeNodeChanges registers with the manager's cfg implementation for notifications on changes to the
// cfgKeySGRCluster key.  When notified, refreshes the handlers nodeIDs.
func (l *ReplicationHeartbeatListener) subscribeNodeSetChanges() error {

	cfgEvents := make(chan cbgt.CfgEvent)

	err := l.mgr.cfg.Subscribe(cfgKeySGRCluster, cfgEvents)
	if err != nil {
		base.DebugfCtx(l.mgr.loggingCtx, base.KeyCluster, "Error subscribing to %s key changes: %v", cfgKeySGRCluster, err)
		return err
	}
	go func() {
		defer base.FatalPanicHandler()
		for {
			select {
			case <-cfgEvents:
				localNodeRegistered, err := l.reloadNodes()
				if err != nil {
					base.WarnfCtx(l.mgr.loggingCtx, "Error while reloading heartbeat node definitions: %v", err)
				}
				if !localNodeRegistered {
					registerErr := l.mgr.RegisterNode(l.mgr.localNodeUUID)
					if registerErr != nil {
						base.WarnfCtx(l.mgr.loggingCtx, "Error attempting to re-register node, node will not participate in sg-replicate until restarted or replication cfg is next updated: %v", registerErr)
					}
				}
			case <-l.terminator:
				return
			}
		}
	}()
	return nil
}

func (l *ReplicationHeartbeatListener) reloadNodes() (localNodePresent bool, err error) {

	nodeSet, err := l.mgr.getNodes()
	if err != nil {
		return false, err
	}

	nodeUUIDs := make([]string, 0)
	if nodeSet != nil {
		for _, nodeDef := range nodeSet {
			nodeUUIDs = append(nodeUUIDs, nodeDef.UUID)
			if nodeDef.UUID == l.mgr.localNodeUUID {
				localNodePresent = true
			}
		}
	}

	l.lock.Lock()
	l.nodeIDs = nodeUUIDs
	l.lock.Unlock()

	return localNodePresent, nil
}

// GetNodes returns a copy of the in-memory node set
func (l *ReplicationHeartbeatListener) GetNodes() ([]string, error) {

	l.lock.RLock()
	nodeIDsCopy := make([]string, len(l.nodeIDs))
	copy(nodeIDsCopy, l.nodeIDs)
	l.lock.RUnlock()
	return nodeIDsCopy, nil
}

func (l *ReplicationHeartbeatListener) Stop() {
	close(l.terminator)
}
