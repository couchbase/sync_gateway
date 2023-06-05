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
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

const (
	// Number of revisions to include in a 'changes' message
	BlipDefaultBatchSize = uint64(200)
	BlipMinimumBatchSize = uint64(10) // Not in the replication spec - is this required?

	// Number of goroutines handling incoming BLIP requests (and other tasks)
	BlipThreadPoolSize = 5

	// Maximum total size of incoming BLIP requests that are currently being dispatched and handled.
	// Above this amount, the BLIP engine stops reading from the WebSocket, applying back-pressure
	// to the client and keeping memory usage down.
	BlipMaxIncomingBytesBeingDispatched = 100000 // bytes

	// Max number of outgoing revisions in memory being sent
	BlipMaxRevsSending = 50

	// Max total size (bytes) of outgoing revisions in memory being sent
	BlipMaxRevsLengthSending = 100 * 1000
)

var ErrClosedBLIPSender = errors.New("use of closed BLIP sender")

func NewBlipSyncContext(ctx context.Context, bc *blip.Context, db *Database, contextID string, replicationStats *BlipSyncStats) *BlipSyncContext {
	bsc := &BlipSyncContext{
		blipContext:             bc,
		blipContextDb:           db,
		loggingCtx:              ctx,
		terminator:              make(chan bool),
		userChangeWaiter:        db.NewUserWaiter(),
		sgCanUseDeltas:          db.DeltaSyncEnabled(),
		replicationStats:        replicationStats,
		inFlightChangesThrottle: make(chan struct{}, maxInFlightChangesBatches),
		collections:             &blipCollections{},
		threadPool:              blip.ThreadPool{Concurrency: BlipThreadPoolSize},
	}
	if bsc.replicationStats == nil {
		bsc.replicationStats = NewBlipSyncStats()
	}
	bsc.stats.lastReportTime.Store(time.Now().UnixMilli())
	bsc.revSender = newBlipRevSender(bsc, BlipMaxRevsSending, BlipMaxRevsLengthSending)

	if u := db.User(); u != nil {
		bsc.userName = u.Name()
		u.InitializeRoles()
		if u.Name() == "" && db.IsGuestReadOnly() {
			bsc.readOnly = true
		}
	}

	// Register default handlers
	bc.FatalErrorHandler = func(err error) {
		base.InfofCtx(ctx, base.KeyHTTP, "%s:     --> BLIP+WebSocket connection error: %v", contextID, err)
	}

	dispatcher := &blip.ByProfileDispatcher{}
	dispatcher.SetDefaultHandler(bsc.NotFoundHandler)

	// Register 2.x replicator handlers
	for profile, handlerFn := range handlersByProfile {
		bsc.register(dispatcher, profile, handlerFn)
	}
	if db.Options.UnsupportedOptions.ConnectedClient {
		// Register Connected Client handlers
		for profile, handlerFn := range kConnectedClientHandlersByProfile {
			bsc.register(dispatcher, profile, handlerFn)
		}
	}
	bsc.blipContext.RequestHandler = dispatcher.Dispatch
	bsc.threadPool.Start()

	return bsc
}

// BlipSyncContext represents one BLIP connection (socket) opened by a client.
// This connection remains open until the client closes it, and can receive any number of requests.
type BlipSyncContext struct {
	blipContext                 *blip.Context
	threadPool                  blip.ThreadPool
	blipContextDb               *Database       // 'master' database instance for the replication, used as source when creating handler-specific databases
	loggingCtx                  context.Context // logging context for connection
	dbUserLock                  sync.RWMutex    // Must be held when refreshing the db user
	allowedAttachments          map[string]AllowedAttachment
	allowedAttachmentsLock      sync.Mutex
	handlerSerialNumber         uint64            // Each handler within a context gets a unique serial number for logging
	terminatorOnce              sync.Once         // Used to ensure the terminator channel below is only ever closed once.
	terminator                  chan bool         // Closed during BlipSyncContext.close(). Ensures termination of async goroutines.
	useDeltas                   bool              // Whether deltas can be used for this connection - This should be set via setUseDeltas()
	sgCanUseDeltas              bool              // Whether deltas can be used by Sync Gateway for this connection
	userChangeWaiter            *ChangeWaiter     // Tracks whether the users/roles associated with the replication have changed
	userName                    string            // Avoid contention on db.user during userChangeWaiter user lookup
	replicationStats            *BlipSyncStats    // Replication stats
	purgeOnRemoval              bool              // Purges the document when we pull a _removed:true revision.
	conflictResolver            *ConflictResolver // Conflict resolver for active replications
	changesPendingResponseCount int64             // Number of changes messages pending changesResponse
	// TODO: For review, whether sendRevAllConflicts needs to be per sendChanges invocation
	sendRevNoConflicts bool                      // Whether to set noconflicts=true when sending revisions
	clientType         BLIPSyncContextClientType // Can perform client-specific replication behaviour based on this field
	// inFlightChangesThrottle is a small buffered channel to limit the amount of in-flight changes batches for this connection.
	// Couchbase Lite limits this on the client side, but this is defensive to prevent other non-CBL clients from requesting too many changes
	// before they've processed the revs for previous batches. Keeping this >1 allows the client to be fed a constant supply of rev messages,
	// without making Sync Gateway buffer a bunch of stuff in memory too far in advance of the client being able to receive the revs.
	inFlightChangesThrottle chan struct{}

	// fatalErrorCallback is called by the replicator code when the replicator using this blipSyncContext should be
	// stopped
	fatalErrorCallback func(err error)

	// when readOnly is true, handleRev requests are rejected
	readOnly bool

	collections *blipCollections // all collections handled by blipSyncContext, implicit or via GetCollections
	revSender   *blipRevSender   // schedules sending 'rev' messages
	stats       blipSyncStats    // internal structure to store stats
}

// blipSyncStats has support structures to support reporting stats at regular interval
type blipSyncStats struct {
	bytesSent      atomic.Uint64 // Total bytes sent to client
	bytesReceived  atomic.Uint64 // Total bytes received from client
	lastReportTime atomic.Int64  // last time reported by time.Time     // Last time blip stats were reported
	lock           sync.Mutex
}

// AllowedAttachment contains the metadata for handling allowed attachments
// while replicating over BLIP protocol.
type AllowedAttachment struct {
	version int    // Version of the attachment
	counter int    // Counter to track allowed attachments
	docID   string // docID, used for BlipCBMobileReplicationV2 retrieval of V2 attachments
}

func (bsc *BlipSyncContext) SetClientType(clientType BLIPSyncContextClientType) {
	bsc.clientType = clientType
}

// Registers a BLIP handler including the outer-level work of logging & error handling.
// Includes the outer handler as a nested function.
func (bsc *BlipSyncContext) register(dispatcher *blip.ByProfileDispatcher, profile string, handlerFn func(*blipHandler, *blip.Message) error) {

	// Wrap the handler function with a function that adds handling needed by all handlers
	handler := func(rq *blip.Message, onComplete func()) {
		defer onComplete()

		// Recover to log panic from handlers and repanic for go-blip response handling
		defer func() {
			if err := recover(); err != nil {

				// If we recover from a panic and the database bucket has gone - we likely paniced due to a config update causing a db reload.
				// Until we have a better way of telling a client this has happened and to reconnect, returning a 503 will cause the client to reconnect.
				if bsc.blipContextDb.DatabaseContext.Bucket == nil {
					base.InfofCtx(bsc.loggingCtx, base.KeySync, "Database bucket closed underneath request %v - asking client to reconnect", rq)
					base.DebugfCtx(bsc.loggingCtx, base.KeySync, "PANIC handling BLIP request %v: %v\n%s", rq, err, debug.Stack())
					// HTTP 503 asks CBL to disconnect and retry.
					rq.Response().SetError("HTTP", ErrDatabaseWentAway.Status, ErrDatabaseWentAway.Message)
					return
				}

				// This is a panic we don't know about - so continue to log at warn with a generic 500 response via go-blip
				bsc.replicationStats.NumHandlersPanicked.Add(1)
				base.WarnfCtx(bsc.loggingCtx, "PANIC handling BLIP request %v: %v\n%s", rq, err, debug.Stack())
				panic(err)
			}
		}()

		startTime := time.Now()
		handler := blipHandler{
			BlipSyncContext: bsc,
			db:              bsc.copyContextDatabase(),
			serialNumber:    bsc.incrementSerialNumber(),
		}

		// Trace log the full message body and properties
		if base.LogTraceEnabled(base.KeySyncMsg) {
			rqBody, _ := rq.Body()
			base.TracefCtx(bsc.loggingCtx, base.KeySyncMsg, "Recv Req %s: Body: '%s' Properties: %v", rq, base.UD(rqBody), base.UD(rq.Properties))
		}

		if err := handlerFn(&handler, rq); err != nil {
			status, msg := base.ErrorAsHTTPStatus(err)
			if response := rq.Response(); response != nil {
				response.SetError("HTTP", status, msg)
			}
			if profile == MessageGetCheckpoint && status == http.StatusNotFound {
				// lower log level for missing checkpoints - it's expected behaviour for new clients
				base.DebugfCtx(bsc.loggingCtx, base.KeySyncMsg, "#%d: Type:%s   --> no existing checkpoint for client Time:%v", handler.serialNumber, profile, time.Since(startTime))
			} else {
				base.InfofCtx(bsc.loggingCtx, base.KeySyncMsg, "#%d: Type:%s   --> %d %s Time:%v", handler.serialNumber, profile, status, msg, time.Since(startTime))
			}
		} else if profile != MessageSubChanges {
			// Log the fact that the handler has finished, except for the "subChanges" special case which does it's own termination related logging
			base.DebugfCtx(bsc.loggingCtx, base.KeySyncMsg, "#%d: Type:%s   --> OK Time:%v", handler.serialNumber, profile, time.Since(startTime))
		}

		// Trace log the full response body and properties
		if base.LogTraceEnabled(base.KeySyncMsg) {
			resp := rq.Response()
			if resp == nil {
				return
			}
			respBody, _ := resp.Body()
			base.TracefCtx(bsc.loggingCtx, base.KeySyncMsg, "Recv Rsp %s: Body: '%s' Properties: %v", resp, base.UD(respBody), base.UD(resp.Properties))
		}

		bsc.reportStats(false)
	}

	// Handlers run on the thread pool
	handler = bsc.threadPool.WrapAsyncHandler(handler)

	if concurrency := handlerConcurrencyByProfile[profile]; concurrency > 0 {
		// Limit number of concurrently running handlers for some profiles:
		throttle := blip.ThrottlingDispatcher{
			Handler:        handler,
			MaxConcurrency: concurrency,
		}
		handler = throttle.Dispatch
	}

	dispatcher.SetHandler(profile, handler)
}

func (bsc *BlipSyncContext) Close() {
	bsc.terminatorOnce.Do(func() {
		for _, collection := range bsc.collections.getAll() {
			// if initial GetCollections returned an invalid collections, this will be nil
			if collection == nil {
				continue
			}
			// Lock so that we don't close the changesCtx at the same time as handleSubChanges is creating it
			collection.changesCtxLock.Lock()
			defer collection.changesCtxLock.Unlock()

			collection.changesCtxCancel()
		}
		bsc.reportStats(true)
		bsc.threadPool.Stop()
		close(bsc.terminator)
	})
}

// NotFoundHandler is used for unknown requests
func (bsc *BlipSyncContext) NotFoundHandler(rq *blip.Message, onComplete func()) {
	base.InfofCtx(bsc.loggingCtx, base.KeySync, "%s Type:%q", rq, rq.Profile())
	base.InfofCtx(bsc.loggingCtx, base.KeySync, "%s    --> 404 Unknown profile", rq)
	blip.UnhandledAsync(rq, onComplete)
}

func (bsc *BlipSyncContext) copyContextDatabase() *Database {
	bsc.dbUserLock.RLock()
	databaseCopy := bsc._copyContextDatabase()
	bsc.dbUserLock.RUnlock()
	return databaseCopy
}

func (bsc *BlipSyncContext) copyDatabaseCollectionWithUser(collectionIdx *int) (*DatabaseCollectionWithUser, error) {
	bsc.dbUserLock.RLock()
	defer bsc.dbUserLock.RUnlock()
	user := bsc.blipContextDb.User()
	collectionCtx, err := bsc.collections.get(collectionIdx)
	if err != nil {
		return nil, err
	}
	return &DatabaseCollectionWithUser{DatabaseCollection: collectionCtx.dbCollection, user: user}, nil
}

func (bsc *BlipSyncContext) _copyContextDatabase() *Database {
	databaseCopy, _ := GetDatabase(bsc.blipContextDb.DatabaseContext, bsc.blipContextDb.User())
	return databaseCopy
}

// Handles the response to a pushed "changes" message, i.e. the list of revisions the client wants
func (bsc *BlipSyncContext) handleChangesResponse(sender *blip.Sender, response *blip.Message, changeArray [][]interface{}, requestSent time.Time, collectionIdx *int) error {
	defer func() {
		if panicked := recover(); panicked != nil {
			bsc.replicationStats.NumHandlersPanicked.Add(1)
			base.WarnfCtx(bsc.loggingCtx, "PANIC handling 'changes' response: %v\n%s", panicked, debug.Stack())
		}
	}()

	respBody, err := response.Body()
	if err != nil {
		base.ErrorfCtx(bsc.loggingCtx, "Couldn't get body for 'changes' response message: %s -- %s", response, err)
		return err
	}

	if response.Type() == blip.ErrorType {
		return fmt.Errorf("Client returned error in changesResponse: %s", respBody)
	}

	var answer []interface{}
	if len(respBody) > 0 {
		if err := base.JSONUnmarshal(respBody, &answer); err != nil {
			base.ErrorfCtx(bsc.loggingCtx, "Invalid response to 'changes' message: %s -- %s.  Body: %s", response, err, respBody)
			return nil
		}
	} else {
		base.DebugfCtx(bsc.loggingCtx, base.KeyAll, "Empty response to 'changes' message: %s", response)
	}
	changesResponseReceived := time.Now()

	bsc.replicationStats.HandleChangesResponseCount.Add(1)
	bsc.replicationStats.HandleChangesResponseTime.Add(time.Since(requestSent).Nanoseconds())

	maxHistory := 0
	if max, err := strconv.ParseUint(response.Properties[ChangesResponseMaxHistory], 10, 64); err == nil {
		maxHistory = int(max)
	}

	// Set useDeltas if the client has delta support and has it enabled
	if clientDeltasStr, ok := response.Properties[ChangesResponseDeltas]; ok {
		bsc.setUseDeltas(clientDeltasStr == trueProperty)
	} else {
		base.TracefCtx(bsc.loggingCtx, base.KeySync, "Client didn't specify 'deltas' property in 'changes' response. useDeltas: %v", bsc.useDeltas)
	}

	// `answer` is an array where each item is either an array of known rev IDs, or a non-array
	// placeholder (probably 0). The item numbers match those of changeArray.
	revsToSend := make([]*revToSend, 0, len(answer))
	sentSeqs := make([]SequenceID, 0)
	alreadyKnownSeqs := make([]SequenceID, 0)

	collectionCtx, err := bsc.collections.get(collectionIdx)
	if err != nil {
		return err
	}

	for i, knownRevsArrayInterface := range answer {
		seq := changeArray[i][0].(SequenceID)
		docID := changeArray[i][1].(string)
		revID := changeArray[i][2].(string)
		if knownRevsArray, ok := knownRevsArrayInterface.([]interface{}); ok {
			revsToSend = append(revsToSend, &revToSend{
				seq:           seq,
				docID:         docID,
				revID:         revID,
				knownRevs:     knownRevsArray,
				maxHistory:    maxHistory,
				useDelta:      bsc.useDeltas,
				collectionIdx: collectionIdx,
				sender:        sender,
				timestamp:     changesResponseReceived,
			})
			if collectionCtx.sgr2PushAddExpectedSeqsCallback != nil {
				sentSeqs = append(sentSeqs, seq)
			}
		} else {
			base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Peer didn't want revision %s / %s (rev.seq:%v)", base.UD(docID), revID, seq)
			if collectionCtx.sgr2PushAlreadyKnownSeqsCallback != nil {
				alreadyKnownSeqs = append(alreadyKnownSeqs, seq)
			}
		}
	}

	if collectionCtx.sgr2PushAlreadyKnownSeqsCallback != nil {
		collectionCtx.sgr2PushAlreadyKnownSeqsCallback(alreadyKnownSeqs...)
	}

	if len(revsToSend) > 0 {
		bsc.revSender.addRevs(revsToSend)
		if collectionCtx.sgr2PushAddExpectedSeqsCallback != nil {
			collectionCtx.sgr2PushAddExpectedSeqsCallback(sentSeqs...)
		}
		bsc.replicationStats.HandleChangesSendRevTime.Add(time.Since(changesResponseReceived).Nanoseconds())
	}

	return nil
}

// Pushes a revision body to the client. Returns length of body in bytes.
func (bsc *BlipSyncContext) sendRevisionWithProperties(r *revToSend,
	bodyBytes []byte, attMeta []AttachmentStorageMeta, properties blip.Properties, resendFullRevisionFunc func() error) error {

	docID := r.docID
	revID := r.revID
	seq := r.seq

	outrq := NewRevMessage()
	outrq.SetID(docID)
	outrq.SetRev(revID)
	outrq.SetCollection(r.collectionIdx)
	if bsc.sendRevNoConflicts {
		outrq.SetNoConflicts(true)
	}

	// add additional properties passed through
	outrq.SetProperties(properties)

	outrq.SetJSONBodyAsBytes(bodyBytes)
	r.messageLen = len(bodyBytes)
	// Update read stats
	if messageBody, err := outrq.Body(); err == nil {
		bsc.replicationStats.SendRevBytes.Add(int64(len(messageBody)))
	}

	base.TracefCtx(bsc.loggingCtx, base.KeySync, "Sending revision %s/%s, body:%s, properties: %v, attDigests: %v", base.UD(docID), revID, base.UD(string(bodyBytes)), base.UD(properties), attMeta)

	collectionCtx, err := bsc.collections.get(r.collectionIdx)
	if err != nil {
		return err
	}
	// asynchronously wait for a response if we have attachment digests to verify, if we sent a delta and want to error check, or if we have a registered callback.
	awaitResponse := len(attMeta) > 0 || properties[RevMessageDeltaSrc] != "" || collectionCtx.sgr2PushProcessedSeqCallback != nil

	activeSubprotocol := bsc.blipContext.ActiveSubprotocol()
	if awaitResponse {
		// Allow client to download attachments in 'atts', but only while pulling this rev
		bsc.addAllowedAttachments(docID, attMeta, activeSubprotocol)
	} else {
		bsc.replicationStats.SendRevCount.Add(1)
		outrq.SetNoReply(true)
	}

	outrq.OnSent(func() { bsc.revSender.completedRev(r) })

	// send the rev
	if !bsc.sendBLIPMessage(r.sender, outrq.Message) {
		bsc.removeAllowedAttachments(docID, attMeta, activeSubprotocol)
		return ErrClosedBLIPSender
	}

	if awaitResponse {
		outrq.OnResponse(func(resp *blip.Message) {
			bsc.threadPool.Go(func() {
				defer func() {
					if panicked := recover(); panicked != nil {
						bsc.replicationStats.NumHandlersPanicked.Add(1)
						base.WarnfCtx(bsc.loggingCtx, "PANIC handling 'sendRevision' response: %v\n%s", panicked, debug.Stack())
						bsc.Close()
					}
				}()

				respBody, err := resp.Body()
				if err != nil {
					base.WarnfCtx(bsc.loggingCtx, "couldn't get response body for rev: %v", err)
				}

				base.TracefCtx(bsc.loggingCtx, base.KeySync, "Received response for sendRevisionWithProperties rev message %s/%s", base.UD(docID), revID)

				if resp.Type() == blip.ErrorType {
					bsc.replicationStats.SendRevErrorTotal.Add(1)
					base.InfofCtx(bsc.loggingCtx, base.KeySync, "error %s in response to rev: %s", resp.Properties["Error-Code"], respBody)

					if errorDomainIsHTTP(resp) {
						switch resp.Properties["Error-Code"] {
						case "409":
							bsc.replicationStats.SendRevErrorConflictCount.Add(1)
						case "403":
							bsc.replicationStats.SendRevErrorRejectedCount.Add(1)
						case "422", "404":
							// unprocessable entity, CBL has not been able to use the delta we sent, so we should re-send the revision in full
							if resendFullRevisionFunc != nil {
								base.DebugfCtx(bsc.loggingCtx, base.KeySync, "sending full body replication for doc %s/%s due to unprocessable entity", base.UD(docID), revID)
								if err := resendFullRevisionFunc(); err != nil {
									base.WarnfCtx(bsc.loggingCtx, "unable to resend revision: %v", err)
								}
							}
						case "500":
							// runtime exceptions return 500 status codes, but we have no other way to determine if this 500 error was caused by the sync-function than matching on the error message.
							if bytes.Contains(respBody, []byte("JS sync function")) {
								bsc.replicationStats.SendRevErrorRejectedCount.Add(1)
							} else {
								bsc.replicationStats.SendRevErrorOtherCount.Add(1)
							}
						}
					}
				} else {
					bsc.replicationStats.SendRevCount.Add(1)
				}

				bsc.removeAllowedAttachments(docID, attMeta, activeSubprotocol)

				if collectionCtx.sgr2PushProcessedSeqCallback != nil {
					collectionCtx.sgr2PushProcessedSeqCallback(seq)
				}
			})
		})
	}

	return nil
}

func (bsc *BlipSyncContext) allowedAttachment(digest string) AllowedAttachment {
	bsc.allowedAttachmentsLock.Lock()
	defer bsc.allowedAttachmentsLock.Unlock()
	return bsc.allowedAttachments[digest]
}

// setUseDeltas will set useDeltas on the BlipSyncContext as long as both sides of the connection have it enabled.
func (bsc *BlipSyncContext) setUseDeltas(clientCanUseDeltas bool) {
	if bsc.useDeltas && bsc.sgCanUseDeltas && clientCanUseDeltas {
		// fast-path for deltas that are already enabled and still wanted on both sides.
		return
	}

	if !bsc.useDeltas && !bsc.sgCanUseDeltas && !clientCanUseDeltas {
		// fast-path for deltas that are already disabled and still not wanted on both sides.
		return
	}

	// Both sides want deltas, and we've not previously enabled them.
	if bsc.sgCanUseDeltas && clientCanUseDeltas && !bsc.useDeltas {
		base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Enabling deltas for this replication")
		bsc.replicationStats.DeltaEnabledPullReplicationCount.Add(1)
		bsc.useDeltas = true
		return
	}

	// Below options shouldn't be hit. Delta sync is only turned on once per replication, and never off again.

	// The client doesn't want deltas, but we'd previously enabled them.
	if !clientCanUseDeltas && bsc.useDeltas {
		base.InfofCtx(bsc.loggingCtx, base.KeySync, "Disabling deltas for this replication based on client setting.")
		bsc.useDeltas = false
		return
	}

	// We don't want deltas, but we'd previously enabled them.
	if !bsc.sgCanUseDeltas && bsc.useDeltas {
		base.InfofCtx(bsc.loggingCtx, base.KeySync, "Disabling deltas for this replication based on server setting.")
		bsc.useDeltas = false
		return
	}
}

func (bsc *BlipSyncContext) sendDelta(r *revToSend, deltaSrcRevID string, revDelta *RevisionDelta, resendFullRevisionFunc func() error) error {

	properties := blipRevMessageProperties(revDelta.RevisionHistory, revDelta.ToDeleted, r.seq)
	properties[RevMessageDeltaSrc] = deltaSrcRevID

	base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Sending rev %q %s as delta. DeltaSrc:%s", base.UD(r.docID), revDelta.ToRevID, deltaSrcRevID)
	return bsc.sendRevisionWithProperties(r, revDelta.DeltaBytes, revDelta.AttachmentStorageMeta,
		properties, resendFullRevisionFunc)
}

// sendBLIPMessage is a simple wrapper around all sent BLIP messages
func (bsc *BlipSyncContext) sendBLIPMessage(sender *blip.Sender, msg *blip.Message) bool {
	ok := sender.Send(msg)
	if base.LogTraceEnabled(base.KeySyncMsg) {
		rqBody, _ := msg.Body()
		base.TracefCtx(bsc.loggingCtx, base.KeySyncMsg, "Sent Req %s: Body: '%s' Properties: %v", msg, base.UD(rqBody), base.UD(msg.Properties))
	}
	return ok
}

func (bsc *BlipSyncContext) sendNoRev(r *revToSend, err error) error {
	base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Sending norev %q %s due to unavailable revision: %v", base.UD(r.docID), r.revID, err)

	noRevRq := NewNoRevMessage()
	noRevRq.SetId(r.docID)
	noRevRq.SetRev(r.revID)
	noRevRq.SetCollection(r.collectionIdx)
	if bsc.blipContext.ActiveSubprotocol() == BlipCBMobileReplicationV2 && bsc.clientType == BLIPClientTypeSGR2 {
		noRevRq.SetSeq(r.seq)
	} else {
		noRevRq.SetSequence(r.seq)
	}

	status, reason := base.ErrorAsHTTPStatus(err)
	noRevRq.SetError(strconv.Itoa(status))

	// Add a "reason" field that gives more detailed explanation on the cause of the error.
	noRevRq.SetReason(reason)

	noRevRq.SetNoReply(true)
	noRevRq.OnSent(func() { bsc.revSender.completedRev(r) })
	if !bsc.sendBLIPMessage(r.sender, noRevRq.Message) {
		return ErrClosedBLIPSender
	}

	collectionCtx, err := bsc.collections.get(r.collectionIdx)
	if err != nil {
		return err
	}

	if collectionCtx.sgr2PushProcessedSeqCallback != nil {
		collectionCtx.sgr2PushProcessedSeqCallback(r.seq)
	}

	return nil
}

// Pushes a revision body to the client
func (bsc *BlipSyncContext) sendRevision(collection *DatabaseCollectionWithUser, r *revToSend, knownRevs map[string]bool) error {
	rev, err := collection.GetRev(bsc.loggingCtx, r.docID, r.revID, true, nil)
	if err != nil {
		if !base.IsDocNotFoundError(err) {
			err = fmt.Errorf("failed to GetRev for doc %s with rev %s: %w", base.UD(r.docID).Redact(), base.MD(r.revID).Redact(), err)
		}
		return err
	}

	base.TracefCtx(bsc.loggingCtx, base.KeySync, "sendRevision, rev attachments for %s/%s are %v", base.UD(r.docID), r.revID, base.UD(rev.Attachments))
	attachmentStorageMeta := ToAttachmentStorageMeta(rev.Attachments)
	var bodyBytes []byte
	if base.IsEnterpriseEdition() {
		// Still need to stamp _attachments into BLIP messages
		if len(rev.Attachments) > 0 {
			DeleteAttachmentVersion(rev.Attachments)
			bodyBytes, err = base.InjectJSONProperties(rev.BodyBytes, base.KVPair{Key: BodyAttachments, Val: rev.Attachments})
			if err != nil {
				return err
			}
		} else {
			bodyBytes = rev.BodyBytes
		}
	} else {
		body, err := rev.Body()
		if err != nil {
			return err
		}

		// Still need to stamp _attachments into BLIP messages
		if len(rev.Attachments) > 0 {
			DeleteAttachmentVersion(rev.Attachments)
			body[BodyAttachments] = rev.Attachments
		}

		bodyBytes, err = base.JSONMarshalCanonical(body)
		if err != nil {
			return err
		}
	}

	history := toHistory(rev.History, knownRevs, r.maxHistory)
	properties := blipRevMessageProperties(history, rev.Deleted, r.seq)
	if base.LogDebugEnabled(base.KeySync) {
		base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Sending rev %q %s based on %d known, digests: %v", base.UD(r.docID), r.revID, len(r.knownRevs), digests(attachmentStorageMeta))
	}
	return bsc.sendRevisionWithProperties(r, bodyBytes, attachmentStorageMeta, properties, nil)
}

// digests returns a slice of digest extracted from the given attachment meta.
func digests(meta []AttachmentStorageMeta) []string {
	digests := make([]string, len(meta))
	for _, m := range meta {
		digests = append(digests, m.digest)
	}
	return digests
}

func toHistory(revisions Revisions, knownRevs map[string]bool, maxHistory int) []string {
	// Get the revision's history as a descending array of ancestor revIDs:
	history := revisions.ParseRevisions()[1:]
	for i, rev := range history {
		if knownRevs[rev] || (maxHistory > 0 && i+1 >= maxHistory) {
			history = history[0 : i+1]
			break
		} else {
			knownRevs[rev] = true
		}
	}
	return history
}

// timeElapsedForStatsReporting will return true if enough time has passed since the previous report.
func (bsc *BlipSyncContext) timeElapsedForStatsReporting(currentTime int64) bool {
	return (currentTime - bsc.stats.lastReportTime.Load()) >= bsc.blipContextDb.Options.BlipStatsReportingInterval
}

// reportStats will update the stats on a database immediately if updateImmediately is true, otherwise update on BlipStatsReportinInterval
func (bsc *BlipSyncContext) reportStats(updateImmediately bool) {
	if bsc.blipContextDb == nil || bsc.blipContext == nil {
		return
	}
	dbStats := bsc.blipContextDb.DbStats.Database()
	if dbStats == nil {
		return
	}
	currentTime := time.Now().UnixMilli()
	if !updateImmediately && !bsc.timeElapsedForStatsReporting(currentTime) {
		return
	}

	bsc.stats.lock.Lock()
	defer bsc.stats.lock.Unlock()

	// check a second time after acquiring the lock to see stats reporting was slow enough that a waiting mutex doesn't need to run
	if !updateImmediately && !bsc.timeElapsedForStatsReporting(time.Now().UnixMilli()) {
		return
	}

	totalBytesSent := bsc.blipContext.GetBytesSent()
	newBytesSent := totalBytesSent - bsc.stats.bytesSent.Swap(totalBytesSent)
	dbStats.ReplicationBytesSent.Add(int64(newBytesSent))

	totalBytesReceived := bsc.blipContext.GetBytesReceived()
	newBytesReceived := totalBytesReceived - bsc.stats.bytesReceived.Swap(totalBytesReceived)
	dbStats.ReplicationBytesReceived.Add(int64(newBytesReceived))
	bsc.stats.lastReportTime.Store(currentTime)

}
