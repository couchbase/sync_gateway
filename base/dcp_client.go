// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	sgbucket "github.com/couchbase/sg-bucket"
)

//

const openStreamTimeout = 30 * time.Second
const openRetryCount = uint32(10)
const DefaultNumWorkers = 8

// DCP buffer size if we are running in serverless
const DefaultDCPBufferServerless = 1 * 1024 * 1024

const getVbSeqnoTimeout = 30 * time.Second

const infiniteOpenStreamRetries = uint32(math.MaxUint32)

type endStreamCallbackFunc func(e endStreamEvent)

var ErrVbUUIDMismatch = errors.New("VbUUID mismatch when failOnRollback set")

type DCPClient struct {
	ctx                        context.Context
	ID                         string                         // unique ID for DCPClient - used for DCP stream name, must be unique
	agent                      *gocbcore.DCPAgent             // SDK DCP agent, manages connections and calls back to DCPClient stream observer implementation
	callback                   sgbucket.FeedEventCallbackFunc // Callback invoked on DCP mutations/deletions
	workers                    []*DCPWorker                   // Workers for concurrent processing of incoming mutations and callback.  vbuckets are partitioned across workers
	workersWg                  sync.WaitGroup                 // Active workers WG - used for signaling when the DCPClient workers have all stopped so the doneChannel can be closed
	spec                       BucketSpec                     // Bucket spec for the target data store
	supportsCollections        bool                           // Whether the target data store supports collections
	numVbuckets                uint16                         // number of vbuckets on target data store
	terminator                 chan bool                      // Used to close worker goroutines spawned by the DCPClient
	doneChannel                chan error                     // Returns nil on successful completion of one-shot feed or external close of feed, error otherwise
	metadata                   DCPMetadataStore               // Implementation of DCPMetadataStore for metadata persistence
	activeVbuckets             map[uint16]struct{}            // vbuckets that have an open stream
	activeVbucketLock          sync.Mutex                     // Synchronization for activeVbuckets
	oneShot                    bool                           // Whether DCP feed should be one-shot
	closing                    AtomicBool                     // Set when the client is closing (either due to internal or external request)
	closeError                 error                          // Will be set to a non-nil value for unexpected error
	closeErrorLock             sync.Mutex                     // Synchronization on close error
	failOnRollback             bool                           // When true, close when rollback detected
	checkpointPrefix           string                         // DCP checkpoint key prefix
	checkpointPersistFrequency *time.Duration                 // Used to override the default checkpoint persistence frequency
	dbStats                    *expvar.Map                    // Stats for database
	agentPriority              gocbcore.DcpAgentPriority      // agentPriority specifies the priority level for a dcp stream
	collectionIDs              []uint32                       // collectionIDs used by gocbcore, if empty, uses default collections
}

type DCPClientOptions struct {
	NumWorkers                 int
	OneShot                    bool
	FailOnRollback             bool                      // When true, the DCP client will terminate on DCP rollback
	InitialMetadata            []DCPMetadata             // When set, will be used as initial metadata for the DCP feed.  Will override any persisted metadata
	CheckpointPersistFrequency *time.Duration            // Overrides metadata persistence frequency - intended for test use
	MetadataStoreType          DCPMetadataStoreType      // define storage type for DCPMetadata
	GroupID                    string                    // specify GroupID, only used when MetadataStoreType is DCPMetadataCS
	DbStats                    *expvar.Map               // Optional stats
	AgentPriority              gocbcore.DcpAgentPriority // agentPriority specifies the priority level for a dcp stream
	CollectionIDs              []uint32                  // CollectionIDs used by gocbcore, if empty, uses default collections
	CheckpointPrefix           string
}

func NewDCPClient(ctx context.Context, ID string, callback sgbucket.FeedEventCallbackFunc, options DCPClientOptions, bucket *GocbV2Bucket) (*DCPClient, error) {

	numWorkers := DefaultNumWorkers
	if options.NumWorkers > 0 {
		numWorkers = options.NumWorkers
	}

	numVbuckets, err := bucket.GetMaxVbno()
	if err != nil {
		return nil, fmt.Errorf("Unable to determine maxVbNo when creating DCP client: %w", err)
	}

	if options.AgentPriority == gocbcore.DcpAgentPriorityHigh {
		return nil, fmt.Errorf("sync gateway should not set high priority for DCP feeds")
	}

	if options.CheckpointPrefix == "" {
		if options.MetadataStoreType == DCPMetadataStoreCS {
			return nil, fmt.Errorf("callers must specify a checkpoint prefix when persisting metadata")
		}
	}
	client := &DCPClient{
		ctx:                 ctx,
		workers:             make([]*DCPWorker, numWorkers),
		numVbuckets:         numVbuckets,
		callback:            callback,
		ID:                  ID,
		spec:                bucket.GetSpec(),
		supportsCollections: bucket.IsSupported(sgbucket.BucketStoreFeatureCollections),
		terminator:          make(chan bool),
		doneChannel:         make(chan error, 1),
		failOnRollback:      options.FailOnRollback,
		checkpointPrefix:    options.CheckpointPrefix,
		dbStats:             options.DbStats,
		agentPriority:       options.AgentPriority,
		collectionIDs:       options.CollectionIDs,
	}

	// Initialize active vbuckets
	client.activeVbuckets = make(map[uint16]struct{})
	for vbNo := uint16(0); vbNo < numVbuckets; vbNo++ {
		client.activeVbuckets[vbNo] = struct{}{}
	}

	checkpointPrefix := fmt.Sprintf("%s:%v", client.checkpointPrefix, ID)
	switch options.MetadataStoreType {
	case DCPMetadataStoreCS:
		// TODO: Change GetSingleDataStore to a metadata Store?
		metadataStore := bucket.DefaultDataStore()
		client.metadata = NewDCPMetadataCS(ctx, metadataStore, numVbuckets, numWorkers, checkpointPrefix)
	case DCPMetadataStoreInMemory:
		client.metadata = NewDCPMetadataMem(numVbuckets)
	default:
		return nil, fmt.Errorf("Unknown Metadatatype: %d", options.MetadataStoreType)
	}
	if options.InitialMetadata != nil {
		for vbID, meta := range options.InitialMetadata {
			client.metadata.SetMeta(uint16(vbID), meta)
		}
	}
	if len(client.collectionIDs) == 0 {
		client.collectionIDs = []uint32{DefaultCollectionID}
	}

	client.oneShot = options.OneShot

	return client, nil
}

// getCollectionHighSeqNo returns the highSeqNo for a given KV collection ID.
func (dc *DCPClient) getCollectionHighSeqNos(collectionID uint32) ([]uint64, error) {
	vbucketSeqnoOptions := gocbcore.GetVbucketSeqnoOptions{}
	if dc.supportsCollections {
		vbucketSeqnoOptions.FilterOptions = &gocbcore.GetVbucketSeqnoFilterOptions{CollectionID: collectionID}
	}
	configSnapshot, err := dc.agent.ConfigSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to get gocbcore connection config: %w", err)
	}

	numServers, err := configSnapshot.NumServers()
	if err != nil {
		return nil, fmt.Errorf("failed to determine the number of servers in the target cluster: %w", err)
	}
	highSeqNos := make([]uint64, dc.numVbuckets)
	// each server is going to return correct values for the active vbuckets on that node,
	// so loop over all servers and take the max values
	// serverIdx start at 1 (-1 in gocbcore), 0 refers to the master node
	for serverIdx := 1; serverIdx <= numServers; serverIdx++ {

		// This has to be buffered so that Cancel() below doesn't lead to blocking in the callback.
		// (If Cancel succeeds then it will lead to directly calling the callback).
		highSeqNoError := make(chan error, 1)
		highSeqNoCallback := func(entries []gocbcore.VbSeqNoEntry, err error) {
			if err == nil {
				for _, entry := range entries {
					if highSeqNos[entry.VbID] < uint64(entry.SeqNo) {
						highSeqNos[entry.VbID] = uint64(entry.SeqNo)
					}
				}
			}
			highSeqNoError <- err
		}
		op, seqErr := dc.agent.GetVbucketSeqnos(
			serverIdx,
			memd.VbucketStateActive, // active vbuckets only
			vbucketSeqnoOptions,     // contains collectionID
			highSeqNoCallback)

		if seqErr != nil {
			return nil, seqErr
		}

		select {
		case err := <-highSeqNoError:
			if err != nil {
				return nil, err
			}
		case <-time.After(getVbSeqnoTimeout):
			op.Cancel()
			<-highSeqNoError
			return nil, ErrTimeout
		}
	}
	return highSeqNos, nil
}

// getHighSeqNos returns the maximum sequence number for every collection configured by the DCP agent.
func (dc *DCPClient) getHighSeqNos() ([]uint64, error) {
	highSeqNos := make([]uint64, dc.numVbuckets)
	// Initialize highSeqNo to the current metadata's StartSeqNo - we don't want to use a value lower than what
	// we've already processed
	for vbNo := uint16(0); vbNo < dc.numVbuckets; vbNo++ {
		highSeqNos[vbNo] = uint64(dc.metadata.GetMeta(vbNo).StartSeqNo)
	}
	for _, collectionID := range dc.collectionIDs {
		colHighSeqNos, err := dc.getCollectionHighSeqNos(collectionID)
		if err != nil {
			return nil, err
		}
		for i, colHighSeqNo := range colHighSeqNos {
			if colHighSeqNo > highSeqNos[i] {
				highSeqNos[i] = colHighSeqNo
			}
		}
	}
	return highSeqNos, nil
}

// configureOneShot sets highSeqnos for a one shot feed.
func (dc *DCPClient) configureOneShot() error {
	highSeqNos, err := dc.getHighSeqNos()
	if err != nil {
		return err
	}

	// Set endSeqNos on client metadata for use when opening streams
	endSeqNos := make(map[uint16]uint64, dc.numVbuckets)
	for vbNo, highSeqNo := range highSeqNos {
		endSeqNos[uint16(vbNo)] = highSeqNo
	}
	dc.metadata.SetEndSeqNos(endSeqNos)
	return nil
}

// Start returns an error and a channel to indicate when the DCPClient is done. If Start returns an error, DCPClient.Close() needs to be called.
func (dc *DCPClient) Start() (doneChan chan error, err error) {
	err = dc.initAgent(dc.spec)
	if err != nil {
		return dc.doneChannel, err
	}
	if dc.oneShot {
		err = dc.configureOneShot()
		if err != nil {
			return dc.doneChannel, err
		}
	}
	dc.startWorkers(dc.ctx)

	for i := uint16(0); i < dc.numVbuckets; i++ {
		openErr := dc.openStream(i, openRetryCount)
		if openErr != nil {
			return dc.doneChannel, fmt.Errorf("Unable to start DCP client, error opening stream for vb %d: %w", i, openErr)
		}
	}
	return dc.doneChannel, nil
}

// Close is used externally to stop the DCP client. If the client was already closed due to error, returns that error
func (dc *DCPClient) Close() error {
	dc.close()
	return dc.getCloseError()
}

// GetMetadata returns metadata for all vbuckets
func (dc *DCPClient) GetMetadata() []DCPMetadata {
	metadata := make([]DCPMetadata, dc.numVbuckets)
	for i := uint16(0); i < dc.numVbuckets; i++ {
		metadata[i] = dc.metadata.GetMeta(i)
	}
	return metadata
}

// close is used internally to stop the DCP client.  Sends any fatal errors to the client's done channel, and
// closes that channel.
func (dc *DCPClient) close() {

	// set dc.closing to true, avoid re-triggering close if it's already in progress
	if !dc.closing.CompareAndSwap(false, true) {
		InfofCtx(dc.ctx, KeyDCP, "DCP Client close called - client is already closing")
		return
	}

	// Stop workers
	close(dc.terminator)
	if dc.agent != nil {
		agentErr := dc.agent.Close()
		if agentErr != nil {
			WarnfCtx(dc.ctx, "Error closing DCP agent in client close: %v", agentErr)
		}
	}

	// Wait for all workers to finish before closing doneChannel
	go func() {
		dc.workersWg.Wait()
		dc.doneChannel <- dc.getCloseError()
		close(dc.doneChannel)
	}()
}

func (dc *DCPClient) initAgent(spec BucketSpec) error {
	defaultValues := &GoCBConnStringParams{
		KVPoolSize:    GoCBPoolSizeDCP,
		KVBufferSize:  spec.KvBufferSize,
		DCPBufferSize: spec.DcpBuffer,
	}

	// Force poolsize to 1, multiple clients results in DCP naming collision
	connStr, err := spec.GetGoCBConnString(defaultValues, IntPtr(GoCBPoolSizeDCP))
	if err != nil {
		return err
	}

	agentConfig := gocbcore.DCPAgentConfig{}
	connStrError := agentConfig.FromConnStr(connStr)
	if connStrError != nil {
		return fmt.Errorf("Unable to start DCP Client - error building conn str: %v", connStrError)
	}

	auth, authErr := spec.GocbcoreAuthProvider()
	if authErr != nil {
		return fmt.Errorf("Unable to start DCP Client - error creating authenticator: %w", authErr)
	}

	tlsRootCAProvider, err := GoCBCoreTLSRootCAProvider(dc.ctx, &spec.TLSSkipVerify, spec.CACertPath)
	if err != nil {
		return err
	}

	agentConfig.BucketName = spec.BucketName
	agentConfig.DCPConfig.AgentPriority = dc.agentPriority
	agentConfig.SecurityConfig.Auth = auth
	agentConfig.SecurityConfig.TLSRootCAProvider = tlsRootCAProvider
	agentConfig.UserAgent = "SyncGatewayDCP"
	if dc.supportsCollections {
		agentConfig.IoConfig = gocbcore.IoConfig{
			UseCollections: true,
		}
	}
	flags := memd.DcpOpenFlagProducer
	flags |= memd.DcpOpenFlagIncludeXattrs
	var agentErr error
	dc.agent, agentErr = gocbcore.CreateDcpAgent(&agentConfig, dc.ID, flags)
	if agentErr != nil {
		return fmt.Errorf("Unable to start DCP client - error creating agent: %w", agentErr)
	}

	// Wait for agent to be ready
	var waitError error
	for i := 0; i < 10; i++ {
		waitError = nil
		agentReadyErr := make(chan error)
		_, err = dc.agent.WaitUntilReady(
			time.Now().Add(30*time.Second),
			gocbcore.WaitUntilReadyOptions{},
			func(_ *gocbcore.WaitUntilReadyResult, err error) {
				agentReadyErr <- err
			})
		if err != nil {
			waitError = fmt.Errorf("WaitUntilReady for dcp agent returned error (%d): %w", i, err)
			continue
		}
		err = <-agentReadyErr
		if err != nil {
			waitError = fmt.Errorf("WaitUntilReady error channel for dcp agent returned error (%d): %w", i, err)
			continue
		}
		if waitError == nil {
			break
		}

	}
	if waitError != nil {
		return waitError
	}

	return nil
}

func (dc *DCPClient) workerForVbno(vbNo uint16) *DCPWorker {
	workerIndex := int(vbNo % uint16(len(dc.workers)))
	return dc.workers[workerIndex]
}

// startWorkers initializes the DCP workers to receive stream events from eventFeed
func (dc *DCPClient) startWorkers(ctx context.Context) {

	// vbuckets are assigned to workers as vbNo % NumWorkers.  Create set of assigned vbuckets
	assignedVbs := make(map[int][]uint16)
	for workerIndex, _ := range dc.workers {
		assignedVbs[workerIndex] = make([]uint16, 0)
	}

	for vbNo := uint16(0); vbNo < dc.numVbuckets; vbNo++ {
		workerIndex := int(vbNo % uint16(len(dc.workers)))
		assignedVbs[workerIndex] = append(assignedVbs[workerIndex], vbNo)
	}

	//
	for index, _ := range dc.workers {
		options := &DCPWorkerOptions{
			metaPersistFrequency: dc.checkpointPersistFrequency,
		}
		dc.workers[index] = NewDCPWorker(index, dc.metadata, dc.callback, dc.onStreamEnd, dc.terminator, nil, dc.checkpointPrefix, assignedVbs[index], options)
		dc.workers[index].Start(ctx, &dc.workersWg)
	}
}

func (dc *DCPClient) openStream(vbID uint16, maxRetries uint32) error {

	var openStreamErr error
	var attempts uint32
	for {
		// Cancel open for stopped client
		select {

		case <-dc.terminator:
			return nil
		default:
		}

		openStreamErr = dc.openStreamRequest(vbID)
		if openStreamErr == nil {
			return nil
		}

		var rollbackErr gocbcore.DCPRollbackError
		switch {
		case errors.As(openStreamErr, &rollbackErr):
			if dc.failOnRollback {
				InfofCtx(dc.ctx, KeyDCP, "Open stream for vbID %d failed due to rollback or range error, closing client based on failOnRollback=true", vbID)
				return fmt.Errorf("%w, failOnRollback requested", openStreamErr)
			}
			InfofCtx(dc.ctx, KeyDCP, "Open stream for vbID %d failed due to rollback or range error, will roll back metadata and retry: %v", vbID, openStreamErr)

			dc.rollback(dc.ctx, vbID, rollbackErr.SeqNo)
		case errors.Is(openStreamErr, gocbcore.ErrMemdRangeError):
			err := fmt.Errorf("Invalid metadata out of range for vbID %d, err: %v metadata %+v, shutting down agent", vbID, openStreamErr, dc.metadata.GetMeta(vbID))
			WarnfCtx(dc.ctx, "%s", err)
			return err
		case errors.Is(openStreamErr, ErrVbUUIDMismatch):
			WarnfCtx(dc.ctx, "Closing Stream for vbID: %d, %s", vbID, openStreamErr)
			return openStreamErr
		case errors.Is(openStreamErr, gocbcore.ErrShutdown):
			WarnfCtx(dc.ctx, "Closing stream for vbID %d, agent has been shut down", vbID)
			return openStreamErr
		case errors.Is(openStreamErr, ErrTimeout):
			InfofCtx(dc.ctx, KeyDCP, "Timeout attempting to open stream for vb %d, will retry", vbID)
		default:
			WarnfCtx(dc.ctx, "Unknown error opening stream for vbID %d: %v", vbID, openStreamErr)
		}
		if maxRetries == infiniteOpenStreamRetries {
			continue
		} else if attempts > maxRetries {
			break
		}
		attempts++
	}

	return fmt.Errorf("openStream failed to complete after %d attempts, last error: %w", attempts, openStreamErr)
}

func (dc *DCPClient) rollback(ctx context.Context, vbID uint16, seqNo gocbcore.SeqNo) {
	if dc.dbStats != nil {
		dc.dbStats.Add("dcp_rollback_count", 1)
	}
	dc.metadata.Rollback(ctx, vbID, seqNo)
}

// openStreamRequest issues the OpenStream request, but doesn't perform any error handling.  Callers
// should generally use openStream() for error and retry handling
func (dc *DCPClient) openStreamRequest(vbID uint16) error {

	vbMeta := dc.metadata.GetMeta(vbID)

	options := gocbcore.OpenStreamOptions{}
	// Always use a collection-aware feed if supported
	if dc.supportsCollections {
		options.FilterOptions = &gocbcore.OpenStreamFilterOptions{CollectionIDs: dc.collectionIDs}
	}

	// This has to be buffered so that Cancel() below doesn't lead to blocking in the callback.
	// (If Cancel succeeds then it will lead to directly calling the callback).
	openStreamError := make(chan error, 1)
	openStreamCallback := func(f []gocbcore.FailoverEntry, err error) {
		if err == nil {
			err = dc.verifyFailoverLog(vbID, f)
			if err == nil {
				dc.metadata.SetFailoverEntries(vbID, f)
			}
		}
		openStreamError <- err
	}

	op, openErr := dc.agent.OpenStream(vbID,
		memd.DcpStreamAddFlagActiveOnly,
		vbMeta.VbUUID,
		vbMeta.StartSeqNo,
		vbMeta.EndSeqNo,
		vbMeta.SnapStartSeqNo,
		vbMeta.SnapEndSeqNo,
		dc,
		options,
		openStreamCallback)

	if openErr != nil {
		return openErr
	}

	select {
	case err := <-openStreamError:
		return err
	case <-time.After(openStreamTimeout):
		op.Cancel()
		<-openStreamError
		return ErrTimeout
	}
}

// verifyFailoverLog checks for VbUUID changes when failOnRollback is set, and
// writes the failover log to the client metadata store.  If previous VbUUID is zero, it's
// not considered a rollback - it's not required to initialize vbUUIDs into meta.
func (dc *DCPClient) verifyFailoverLog(vbID uint16, f []gocbcore.FailoverEntry) error {

	if dc.failOnRollback {
		previousMeta := dc.metadata.GetMeta(vbID)
		// Cases where VbUUID and StartSeqNo aren't set aren't considered rollback
		if previousMeta.VbUUID == 0 && previousMeta.StartSeqNo == 0 {
			return nil
		}

		currentVbUUID := getLatestVbUUID(f)
		// if previousVbUUID hasn't been set yet (is zero), don't treat as rollback.
		if previousMeta.VbUUID != currentVbUUID {
			return ErrVbUUIDMismatch
		}
	}
	return nil
}

func (dc *DCPClient) deactivateVbucket(vbID uint16) {
	dc.activeVbucketLock.Lock()
	delete(dc.activeVbuckets, vbID)
	activeCount := len(dc.activeVbuckets)
	dc.activeVbucketLock.Unlock()
	if activeCount == 0 {
		dc.close()
		// On successful one-shot feed completion, purge persisted checkpoints
		if dc.oneShot {
			dc.metadata.Purge(dc.ctx, len(dc.workers))
		}
	}
}

func (dc *DCPClient) onStreamEnd(e endStreamEvent) {
	if e.err == nil {
		DebugfCtx(dc.ctx, KeyDCP, "Stream (vb:%d) closed, all items streamed", e.vbID)
		dc.deactivateVbucket(e.vbID)
		return
	}

	if errors.Is(e.err, gocbcore.ErrDCPStreamClosed) {
		DebugfCtx(dc.ctx, KeyDCP, "Stream (vb:%d) closed by DCPClient", e.vbID)
		dc.fatalError(fmt.Errorf("Stream (vb:%d) closed by DCPClient", e.vbID))
		return
	}

	if errors.Is(e.err, gocbcore.ErrDCPStreamStateChanged) || errors.Is(e.err, gocbcore.ErrDCPStreamTooSlow) || errors.Is(e.err, gocbcore.ErrDCPStreamDisconnected) {
		DebugfCtx(dc.ctx, KeyDCP, "Stream (vb:%d) ended with a known error, will reconnect. Reason: %s", e.vbID, e.err)
	} else {
		InfofCtx(dc.ctx, KeyDCP, "Stream (vb:%d) ended with an unknown error, will reconnect. Reason: %s", e.vbID, e.err)
	}
	retries := infiniteOpenStreamRetries
	if dc.oneShot {
		retries = openRetryCount
	}

	// Re-opening the stream needs to be asynchronous, due to the way the DCPAgent performs locking while
	// reconfiguring memdclients - the old client can't be closed while it has pending StreamObserver.End calls,
	// and our openStream request won't succeed until the old client is closed.
	// Since we've got a relatively small event buffer for processing observer events (10 x 8 workers), a
	// synchronous openStream request will create a deadlock whenever more than 80 vbuckets need to be closed.
	go func(vb uint16, maxRetries uint32) {
		err := dc.openStream(vb, maxRetries)
		if err != nil {
			dc.fatalError(fmt.Errorf("Stream (vb:%d) failed to reopen: %w", vb, err))
		}
	}(e.vbID, retries)
}

func (dc *DCPClient) fatalError(err error) {
	dc.setCloseError(err)
	dc.close()
}

func (dc *DCPClient) setCloseError(err error) {
	dc.closeErrorLock.Lock()
	defer dc.closeErrorLock.Unlock()
	// If the DCPClient is already closing, don't update the error.  If an initial error triggered the close,
	// then closeError will already be set.  In the event of a requested close, we want to ignore EOF errors associated
	// with stream close
	if dc.closing.IsTrue() {
		return
	}
	if dc.closeError == nil {
		dc.closeError = err
	}
}

func (dc *DCPClient) getCloseError() error {
	dc.closeErrorLock.Lock()
	defer dc.closeErrorLock.Unlock()
	return dc.closeError
}

// getVbUUID returns the VbUUID for the given sequence in the failover log. (the most
// recent failover log entry where log.SeqNo is less than the given sequence)
func getVbUUID(failoverLog []gocbcore.FailoverEntry, seq gocbcore.SeqNo) (vbUUID gocbcore.VbUUID) {
	for i := len(failoverLog) - 1; i >= 0; i-- {
		if failoverLog[i].SeqNo <= seq {
			return failoverLog[i].VbUUID
		}
	}
	return 0
}

// getLatestVbUUID returns the VbUUID associated with the highest sequence in the failover log
func getLatestVbUUID(failoverLog []gocbcore.FailoverEntry) (vbUUID gocbcore.VbUUID) {
	if len(failoverLog) == 0 {
		return 0
	}
	entry := failoverLog[len(failoverLog)-1]
	return entry.VbUUID
}

func (dc *DCPClient) GetMetadataKeyPrefix() string {
	return dc.metadata.GetKeyPrefix()
}
