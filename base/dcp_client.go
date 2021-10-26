package base

import (
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
	ID                string                         // unique ID for DCPClient - used for DCP stream name, must be unique
	agent             *gocbcore.DCPAgent             // SDK DCP agent, manages connections and calls back to DCPClient stream observer implementation
	callback          sgbucket.FeedEventCallbackFunc // Callback invoked on DCP mutations/deletions
	workers           []*DCPWorker                   // Workers for concurrent processing of incoming mutations and callback.  vbuckets are partitioned across workers
	spec              BucketSpec                     // Bucket spec for the target data store
	numVbuckets       uint16                         // number of vbuckets on target data store
	terminator        chan bool                      // Used to close worker goroutines spawned by the DCPClient
	doneChannel       chan error                     // Returns nil on successful completion of one-shot feed or external close of feed, error otherwise
	metadata          DCPMetadataStore               // Implementation of DCPMetadataStore for metadata persistence
	activeVbuckets    map[uint16]struct{}            // vbuckets that have an open stream
	activeVbucketLock sync.Mutex                     // Synchronization for activeVbuckets
	oneShot           bool                           // Whether DCP feed should be one-shot
	endSeqNos         []uint64                       // endSeqNos for one-shot DCP feeds
	closing           AtomicBool                     // Set when the client is closing (either due to internal or external request)
	closeError        error                          // Will be set to a non-nil value for unexpected error
	closeErrorLock    sync.Mutex                     // Synchronization on close error
}

type DCPClientOptions struct {
	NumWorkers  int
	OneShot     bool
	OneShotDone chan struct{}
}

func NewDCPClient(ID string, callback sgbucket.FeedEventCallbackFunc, options DCPClientOptions, store CouchbaseStore) (*DCPClient, error) {

	numWorkers := defaultNumWorkers
	if options.NumWorkers > 0 {
		numWorkers = options.NumWorkers
	}
	numVbuckets, err := store.GetMaxVbno()
	if err != nil {
		return nil, fmt.Errorf("Unable to determine maxVbNo when creating DCP client: %w", err)
	}

	client := &DCPClient{
		workers:     make([]*DCPWorker, numWorkers),
		numVbuckets: numVbuckets,
		callback:    callback,
		ID:          ID,
		spec:        store.GetSpec(),
		terminator:  make(chan bool),
		doneChannel: make(chan error, 1),
	}

	// Initialize active vbuckets
	client.activeVbuckets = make(map[uint16]struct{})
	for vbNo := uint16(0); vbNo < numVbuckets; vbNo++ {
		client.activeVbuckets[vbNo] = struct{}{}
	}

	// TODO: option to specify metadata implementation in DCPClientOptions
	client.metadata = NewDCPMetadataMem(numVbuckets)

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
		if err != nil {
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

// close is used internally to stop the DCP client.  Sends any fatal errors to the client's done channel, and
// closes that channel.
func (dc *DCPClient) close() {

	// set dc.closing to true, avoid retriggering close if it's already in progress
	if !dc.closing.CompareAndSwap(false, true) {
		Infof(KeyDCP, "DCP Client close called - client is already closing")
		return
	}

	// Stop workers
	close(dc.terminator)
	if dc.agent != nil {

		agentErr := dc.agent.Close()
		if agentErr != nil {
			Warnf("Error closing DCP agent in client close: %v", agentErr)
		}
	}
	closeErr := dc.getCloseError()
	if closeErr != nil {
		dc.doneChannel <- closeErr
	}
	close(dc.doneChannel)
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

	// Force poolsize to 1, multiple clients results in DCP naming collision
	agentConfig.KVConfig.PoolSize = 1
	agentConfig.BucketName = spec.BucketName
	agentConfig.DCPConfig.AgentPriority = gocbcore.DcpAgentPriorityLow
	agentConfig.SecurityConfig.Auth = auth
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
		dc.workers[index] = NewDCPWorker(dc.metadata, dc.callback, dc.onStreamEnd, dc.terminator, nil, nil)
		dc.workers[index].Start()
	}
}

func (dc *DCPClient) openStream(vbID uint16) (err error) {

	for i := 0; i < openRetryCount; i++ {
		// Cancel open for stopped client
		select {
		case <-dc.terminator:
			return nil
		default:
		}

		err = dc.openStreamRequest(vbID)
		if err == nil {
			return nil
		}

		switch {
		case errors.Is(err, gocbcore.ErrMemdRollback):
			Infof(KeyDCP, "Open stream for vbID %d failed due to rollback since last open, will rollback metadata and retry", vbID)
			err := dc.rollback(vbID)
			if err != nil {
				return fmt.Errorf("metadata rollback failed for vb %d: %v", vbID, err)
			}
		case errors.Is(err, gocbcore.ErrShutdown):
			Warnf("Closing stream for vbID %d, agent has been shut down", vbID)
			return err
		case errors.Is(err, ErrTimeout):
			Debugf(KeyDCP, "Timeout attempting to open stream for vb %d, will retry", vbID)
		default:
			Warnf("Unexpected error opening stream for vbID %d: %v", vbID, err)
			return err
		}
	}

	return fmt.Errorf("openStream failed to complete after %d attempts, last error: %w", openRetryCount, err)
}

func (dc *DCPClient) rollback(vbID uint16) (err error) {
	// retrieve new vbuuid
	// reset meta
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
			dc.metadata.SetFailoverEntries(vbID, f)
		}
		openStreamError <- err
	}
	_, openErr := dc.agent.OpenStream(vbID,
		memd.DcpStreamAddFlagActiveOnly,
		vbMeta.vbUUID,
		vbMeta.startSeqNo,
		vbMeta.endSeqNo,
		vbMeta.snapStartSeqNo,
		vbMeta.snapEndSeqNo,
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

func (dc *DCPClient) deactivateVbucket(vbID uint16) {
	dc.activeVbucketLock.Lock()
	delete(dc.activeVbuckets, vbID)
	activeCount := len(dc.activeVbuckets)
	dc.activeVbucketLock.Unlock()
	if activeCount == 0 {
		dc.close()
	}
}

func (dc *DCPClient) onStreamEnd(e endStreamEvent) {

	if e.err == nil {
		Debugf(KeyDCP, "Stream (vb:%d) closed, all items streamed", e.vbID)
		dc.deactivateVbucket(e.vbID)
		return
	}

	if errors.Is(e.err, gocbcore.ErrDCPStreamClosed) {
		Debugf(KeyDCP, "Stream (vb:%d) closed by DCPClient", e.vbID)
	}

	if errors.Is(e.err, gocbcore.ErrDCPStreamStateChanged) || errors.Is(e.err, gocbcore.ErrDCPStreamTooSlow) ||
		errors.Is(e.err, gocbcore.ErrDCPStreamDisconnected) {
		Infof(KeyDCP, "Stream (vb:%d) closed by server, will reconnect.  Reason: %v", e.vbID, e.err)
		err := dc.openStream(e.vbID)
		if err != nil {
			dc.fatalError(fmt.Errorf("Stream (vb:%d) failed to reopen: %w", e.vbID, err))
		}
		return
	}

	dc.fatalError(fmt.Errorf("Stream (vb:%d) ended with unknown error: %w", e.vbID, e.err))
}

func (dc *DCPClient) fatalError(err error) {
	Errorf("DCP client failed with error, closing: %v", err)
	dc.setCloseError(err)
	dc.close()
}

func (dc *DCPClient) setCloseError(err error) {
	dc.closeErrorLock.Lock()
	defer dc.closeErrorLock.Unlock()
	if dc.closeError == nil {
		dc.closeError = err
	}
}

func (dc *DCPClient) getCloseError() error {
	dc.closeErrorLock.Lock()
	defer dc.closeErrorLock.Unlock()
	return dc.closeError
}
