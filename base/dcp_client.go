package base

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	sgbucket "github.com/couchbase/sg-bucket"
)

//

const openStreamTimeout = 30 * time.Second
const openRetryCount = 10
const defaultNumWorkers = 8

type endStreamCallbackFunc func(e endStreamEvent)

type DCPClient struct {
	ID                         string                         // unique ID for DCPClient - used for DCP stream name, must be unique
	agent                      *gocbcore.DCPAgent             // SDK DCP agent, manages connections and calls back to DCPClient stream observer implementation
	callback                   sgbucket.FeedEventCallbackFunc // Callback invoked on DCP mutations/deletions
	workers                    []*DCPWorker                   // Workers for concurrent processing of incoming mutations and callback.  vbuckets are partitioned across workers
	workersWg                  sync.WaitGroup                 // Active workers WG - used for signaling when the DCPClient workers have all stopped so the doneChannel can be closed
	spec                       BucketSpec                     // Bucket spec for the target data store
	numVbuckets                uint16                         // number of vbuckets on target data store
	terminator                 chan bool                      // Used to close worker goroutines spawned by the DCPClient
	doneChannel                chan error                     // Returns nil on successful completion of one-shot feed or external close of feed, error otherwise
	metadata                   DCPMetadataStore               // Implementation of DCPMetadataStore for metadata persistence
	activeVbuckets             map[uint16]struct{}            // vbuckets that have an open stream
	activeVbucketLock          sync.Mutex                     // Synchronization for activeVbuckets
	oneShot                    bool                           // Whether DCP feed should be one-shot
	endSeqNos                  []uint64                       // endSeqNos for one-shot DCP feeds
	closing                    AtomicBool                     // Set when the client is closing (either due to internal or external request)
	closeError                 error                          // Will be set to a non-nil value for unexpected error
	closeErrorLock             sync.Mutex                     // Synchronization on close error
	failOnRollback             bool                           // When true, close when rollback detected
	checkpointPrefix           string                         // DCP checkpoint key prefix
	checkpointPersistFrequency *time.Duration                 // Used to override the default checkpoint persistence frequency
}

type DCPClientOptions struct {
	NumWorkers                 int
	OneShot                    bool
	FailOnRollback             bool           // When true, the DCP client will terminate on DCP rollback
	InitialMetadata            []DCPMetadata  // When set, will be used as initial metadata for the DCP feed.  Will override any persisted metadata
	CheckpointPersistFrequency *time.Duration // Overrides metadata persistence frequency - intended for test use
}

func NewDCPClient(ID string, callback sgbucket.FeedEventCallbackFunc, options DCPClientOptions, bucket Bucket, groupID string) (*DCPClient, error) {

	numWorkers := defaultNumWorkers
	if options.NumWorkers > 0 {
		numWorkers = options.NumWorkers
	}

	store, ok := AsCouchbaseStore(bucket)
	if !ok {
		return nil, errors.New("DCP Client requires bucket to be CouchbaseStore")
	}

	numVbuckets, err := store.GetMaxVbno()
	if err != nil {
		return nil, fmt.Errorf("Unable to determine maxVbNo when creating DCP client: %w", err)
	}

	client := &DCPClient{
		workers:          make([]*DCPWorker, numWorkers),
		numVbuckets:      numVbuckets,
		callback:         callback,
		ID:               ID,
		spec:             store.GetSpec(),
		terminator:       make(chan bool),
		doneChannel:      make(chan error, 1),
		failOnRollback:   options.FailOnRollback,
		checkpointPrefix: DCPCheckpointPrefixWithGroupID(groupID),
	}

	// Initialize active vbuckets
	client.activeVbuckets = make(map[uint16]struct{})
	for vbNo := uint16(0); vbNo < numVbuckets; vbNo++ {
		client.activeVbuckets[vbNo] = struct{}{}
	}

	checkpointPrefix := fmt.Sprintf("%s:%v", client.checkpointPrefix, ID)
	client.metadata = NewDCPMetadataCS(bucket, numVbuckets, numWorkers, checkpointPrefix)

	if options.InitialMetadata != nil {
		for vbID, meta := range options.InitialMetadata {
			client.metadata.SetMeta(uint16(vbID), meta)
		}
	}

	if options.OneShot {
		_, highSeqnos, statsErr := store.GetStatsVbSeqno(numVbuckets, true)
		if statsErr != nil {
			return nil, fmt.Errorf("Unable to obtain high seqnos for one-shot DCP feed: %w", statsErr)
		}

		// Set endSeqNos on client for use by stream observer
		client.endSeqNos = make([]uint64, client.numVbuckets)
		for i := uint16(0); i < client.numVbuckets; i++ {
			client.endSeqNos[i] = highSeqnos[i]
		}

		// Set endSeqNos on client metadata for use when opening streams
		client.metadata.SetEndSeqNos(highSeqnos)
		client.oneShot = true
	}

	return client, nil
}

func (dc *DCPClient) Start() (doneChan chan error, err error) {

	err = dc.initAgent(dc.spec)
	if err != nil {
		return nil, err
	}
	dc.startWorkers()

	for i := uint16(0); i < dc.numVbuckets; i++ {
		openErr := dc.openStream(i)
		if openErr != nil {
			return nil, fmt.Errorf("Unable to start DCP client, error opening stream for vb %d: %w", i, openErr)
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
		InfofCtx(context.TODO(), KeyDCP, "DCP Client close called - client is already closing")
		return
	}

	// Stop workers
	close(dc.terminator)
	if dc.agent != nil {
		agentErr := dc.agent.Close()
		if agentErr != nil {
			WarnfCtx(context.TODO(), "Error closing DCP agent in client close: %v", agentErr)
		}
	}
	closeErr := dc.getCloseError()
	if closeErr != nil {
		dc.doneChannel <- closeErr
	}

	// Wait for all workers to finish before closing doneChannel
	go func() {
		dc.workersWg.Wait()
		close(dc.doneChannel)
	}()
}

func (dc *DCPClient) initAgent(spec BucketSpec) error {
	connStr, err := spec.GetGoCBConnString()
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

	tlsRootCAProvider, err := GoCBCoreTLSRootCAProvider(&spec.TLSSkipVerify, spec.CACertPath)
	if err != nil {
		return err
	}

	// Force poolsize to 1, multiple clients results in DCP naming collision
	agentConfig.KVConfig.PoolSize = 1
	agentConfig.BucketName = spec.BucketName
	agentConfig.DCPConfig.AgentPriority = gocbcore.DcpAgentPriorityLow
	agentConfig.SecurityConfig.Auth = auth
	agentConfig.SecurityConfig.TLSRootCAProvider = tlsRootCAProvider
	agentConfig.UserAgent = "SyncGatewayDCP"

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
func (dc *DCPClient) startWorkers() {

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
		dc.workers[index].Start(&dc.workersWg)
	}
}

func (dc *DCPClient) openStream(vbID uint16) (err error) {

	logCtx := context.TODO()
	var openStreamErr error
	for i := 0; i < openRetryCount; i++ {
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

		switch {
		case (errors.Is(openStreamErr, gocbcore.ErrMemdRollback) || errors.Is(openStreamErr, gocbcore.ErrMemdRangeError)):
			if dc.failOnRollback {
				InfofCtx(logCtx, KeyDCP, "Open stream for vbID %d failed due to rollback or range error, closing client based on failOnRollback=true", vbID)
				return fmt.Errorf("%s, failOnRollback requested", openStreamErr)
			}
			InfofCtx(logCtx, KeyDCP, "Open stream for vbID %d failed due to rollback or range error, will roll back metadata and retry: %v", vbID, openStreamErr)
			err := dc.rollback(vbID)
			if err != nil {
				return fmt.Errorf("metadata rollback failed for vb %d: %v", vbID, err)
			}
		case errors.Is(openStreamErr, gocbcore.ErrShutdown):
			WarnfCtx(logCtx, "Closing stream for vbID %d, agent has been shut down", vbID)
			return openStreamErr
		case errors.Is(openStreamErr, ErrTimeout):
			DebugfCtx(logCtx, KeyDCP, "Timeout attempting to open stream for vb %d, will retry", vbID)
		default:
			WarnfCtx(logCtx, "Error opening stream for vbID %d: %v", vbID, openStreamErr)
			return openStreamErr
		}
	}

	return fmt.Errorf("openStream failed to complete after %d attempts, last error: %w", openRetryCount, openStreamErr)
}

func (dc *DCPClient) rollback(vbID uint16) (err error) {
	dc.metadata.Rollback(vbID)
	return nil
}

// openStreamRequest issues the OpenStream request, but doesn't perform any error handling.  Callers
// should generally use openStream() for error and retry handling
func (dc *DCPClient) openStreamRequest(vbID uint16) error {

	vbMeta := dc.metadata.GetMeta(vbID)

	// filter options are only scope + collection selection
	options := gocbcore.OpenStreamOptions{}

	openStreamError := make(chan error)
	openStreamCallback := func(f []gocbcore.FailoverEntry, err error) {
		if err == nil {
			err = dc.verifyAndUpdateFailoverLog(vbID, f)
		}
		openStreamError <- err
	}
	_, openErr := dc.agent.OpenStream(vbID,
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
		return ErrTimeout
	}
}

// verifyAndUpdateFailoverLog checks for VbUUID changes when failOnRollback is set, and
// writes the failover log to the client metadata store.  If previous VbUUID is zero, it's
// not considered a rollback - it's not required to initialize vbUUIDs into meta.
func (dc *DCPClient) verifyAndUpdateFailoverLog(vbID uint16, f []gocbcore.FailoverEntry) error {

	if dc.failOnRollback {
		previousMeta := dc.metadata.GetMeta(vbID)
		// Cases where VbUUID and StartSeqNo aren't set aren't considered rollback
		if previousMeta.VbUUID == 0 && previousMeta.StartSeqNo == 0 {
			dc.metadata.SetFailoverEntries(vbID, f)
			return nil
		}

		currentVbUUID := getLatestVbUUID(f)
		// if previousVbUUID hasn't been set yet (is zero), don't treat as rollback.
		if previousMeta.VbUUID != currentVbUUID {
			return errors.New("VbUUID mismatch when failOnRollback set")
		}
	}
	dc.metadata.SetFailoverEntries(vbID, f)
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
			dc.metadata.Purge(len(dc.workers))
		}
	}
}

func (dc *DCPClient) onStreamEnd(e endStreamEvent) {
	logCtx := context.TODO()

	if e.err == nil {
		DebugfCtx(logCtx, KeyDCP, "Stream (vb:%d) closed, all items streamed", e.vbID)
		dc.deactivateVbucket(e.vbID)
		return
	}

	if errors.Is(e.err, gocbcore.ErrDCPStreamClosed) {
		DebugfCtx(logCtx, KeyDCP, "Stream (vb:%d) closed by DCPClient", e.vbID)
	}

	if errors.Is(e.err, gocbcore.ErrDCPStreamStateChanged) || errors.Is(e.err, gocbcore.ErrDCPStreamTooSlow) ||
		errors.Is(e.err, gocbcore.ErrDCPStreamDisconnected) {
		InfofCtx(logCtx, KeyDCP, "Stream (vb:%d) closed by server, will reconnect.  Reason: %v", e.vbID, e.err)
		err := dc.openStream(e.vbID)
		if err != nil {
			dc.fatalError(fmt.Errorf("Stream (vb:%d) failed to reopen: %w", e.vbID, err))
		}
		return
	}

	dc.fatalError(fmt.Errorf("Stream (vb:%d) ended with unknown error: %w", e.vbID, e.err))
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
