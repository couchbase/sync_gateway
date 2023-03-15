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
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/document"
)

const (
	// Blip default vals
	BlipDefaultBatchSize = uint64(200)
	BlipMinimumBatchSize = uint64(10) // Not in the replication spec - is this required?
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
	}
	if bsc.replicationStats == nil {
		bsc.replicationStats = NewBlipSyncStats()
	}

	if u := db.User(); u != nil {
		bsc.userName = u.Name()
		u.InitializeRoles()
		if u.Name() == "" && db.IsGuestReadOnly() {
			bsc.readOnly = true
		}
	}

	// Register default handlers
	bc.DefaultHandler = bsc.NotFoundHandler
	bc.FatalErrorHandler = func(err error) {
		base.InfofCtx(ctx, base.KeyHTTP, "%s:     --> BLIP+WebSocket connection error: %v", contextID, err)
	}

	// Register 2.x replicator handlers
	for profile, handlerFn := range handlersByProfile {
		bsc.register(profile, handlerFn)
	}

	if db.Options.UnsupportedOptions.ConnectedClient {
		// Register Connected Client handlers
		for profile, handlerFn := range kConnectedClientHandlersByProfile {
			bsc.register(profile, handlerFn)
		}
	}

	return bsc
}

// BlipSyncContext represents one BLIP connection (socket) opened by a client.
// This connection remains open until the client closes it, and can receive any number of requests.
type BlipSyncContext struct {
	blipContext                 *blip.Context
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
func (bsc *BlipSyncContext) register(profile string, handlerFn func(*blipHandler, *blip.Message) error) {

	// Wrap the handler function with a function that adds handling needed by all handlers
	handlerFnWrapper := func(rq *blip.Message) {

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
	}

	bsc.blipContext.HandlerForProfile[profile] = handlerFnWrapper

}

func (bsc *BlipSyncContext) Close() {
	bsc.terminatorOnce.Do(func() {
		for _, collection := range bsc.collections.getAll() {
			// Lock so that we don't close the changesCtx at the same time as handleSubChanges is creating it
			collection.changesCtxLock.Lock()
			defer collection.changesCtxLock.Unlock()

			collection.changesCtxCancel()
		}
		close(bsc.terminator)
	})
}

// NotFoundHandler is used for unknown requests
func (bsc *BlipSyncContext) NotFoundHandler(rq *blip.Message) {
	base.InfofCtx(bsc.loggingCtx, base.KeySync, "%s Type:%q", rq, rq.Profile())
	base.InfofCtx(bsc.loggingCtx, base.KeySync, "%s    --> 404 Unknown profile", rq)
	blip.Unhandled(rq)
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
func (bsc *BlipSyncContext) handleChangesResponse(sender *blip.Sender, response *blip.Message, changeArray [][]interface{}, requestSent time.Time, handleChangesResponseDbCollection *DatabaseCollectionWithUser, collectionIdx *int) error {
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

	// Maps docID --> a map containing true for revIDs known to the client
	knownRevsByDoc := make(map[string]map[string]bool, len(answer))

	// `answer` is an array where each item is either an array of known rev IDs, or a non-array
	// placeholder (probably 0). The item numbers match those of changeArray.
	var revSendTimeLatency int64
	var revSendCount int64
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
			deltaSrcRevID := ""
			knownRevs := knownRevsByDoc[docID]
			if knownRevs == nil {
				knownRevs = make(map[string]bool, len(knownRevsArray))
				knownRevsByDoc[docID] = knownRevs
			}

			// The first element of the knownRevsArray returned from CBL is the parent revision to use as deltaSrc
			if bsc.useDeltas && len(knownRevsArray) > 0 {
				if revID, ok := knownRevsArray[0].(string); ok {
					deltaSrcRevID = revID
				}
			}

			for _, rev := range knownRevsArray {
				if revID, ok := rev.(string); ok {
					knownRevs[revID] = true
				} else {
					base.ErrorfCtx(bsc.loggingCtx, "Invalid response to 'changes' message")
					return nil
				}
			}

			var err error
			if deltaSrcRevID != "" {
				err = bsc.sendRevAsDelta(sender, docID, revID, deltaSrcRevID, seq, knownRevs, maxHistory, handleChangesResponseDbCollection, collectionIdx)
			} else {
				err = bsc.sendRevision(sender, docID, revID, seq, knownRevs, maxHistory, handleChangesResponseDbCollection, collectionIdx)
			}
			if err != nil {
				return err
			}

			revSendTimeLatency += time.Since(changesResponseReceived).Nanoseconds()
			revSendCount++

			if collectionCtx.sgr2PushAddExpectedSeqsCallback != nil {
				sentSeqs = append(sentSeqs, seq)
			}
		} else {
			base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Peer didn't want revision %s / %s (seq:%v)", base.UD(docID), revID, seq)
			if collectionCtx.sgr2PushAlreadyKnownSeqsCallback != nil {
				alreadyKnownSeqs = append(alreadyKnownSeqs, seq)
			}
		}
	}

	if collectionCtx.sgr2PushAlreadyKnownSeqsCallback != nil {
		collectionCtx.sgr2PushAlreadyKnownSeqsCallback(alreadyKnownSeqs...)
	}

	if revSendCount > 0 {
		if collectionCtx.sgr2PushAddExpectedSeqsCallback != nil {
			collectionCtx.sgr2PushAddExpectedSeqsCallback(sentSeqs...)
		}

		bsc.replicationStats.HandleChangesSendRevCount.Add(revSendCount)
		bsc.replicationStats.HandleChangesSendRevLatency.Add(revSendTimeLatency)
		bsc.replicationStats.HandleChangesSendRevTime.Add(time.Since(changesResponseReceived).Nanoseconds())
	}

	return nil
}

// Pushes a revision body to the client
func (bsc *BlipSyncContext) sendRevisionWithProperties(sender *blip.Sender, docID string, revID string, collectionIdx *int,
	bodyBytes []byte, attMeta []AttachmentStorageMeta, properties blip.Properties, seq SequenceID, resendFullRevisionFunc func() error) error {

	outrq := NewRevMessage()
	outrq.SetID(docID)
	outrq.SetRev(revID)
	outrq.SetCollection(collectionIdx)
	if bsc.sendRevNoConflicts {
		outrq.SetNoConflicts(true)
	}

	// add additional properties passed through
	outrq.SetProperties(properties)

	outrq.SetJSONBodyAsBytes(bodyBytes)

	// Update read stats
	if messageBody, err := outrq.Body(); err == nil {
		bsc.replicationStats.SendRevBytes.Add(int64(len(messageBody)))
	}

	base.TracefCtx(bsc.loggingCtx, base.KeySync, "Sending revision %s/%s, body:%s, properties: %v, attDigests: %v", base.UD(docID), revID, base.UD(string(bodyBytes)), base.UD(properties), attMeta)

	collectionCtx, err := bsc.collections.get(collectionIdx)
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

	// send the rev
	if !bsc.sendBLIPMessage(sender, outrq.Message) {
		bsc.removeAllowedAttachments(docID, attMeta, activeSubprotocol)
		return ErrClosedBLIPSender
	}

	if awaitResponse {
		go func(activeSubprotocol string) {
			defer func() {
				if panicked := recover(); panicked != nil {
					bsc.replicationStats.NumHandlersPanicked.Add(1)
					base.WarnfCtx(bsc.loggingCtx, "PANIC handling 'sendRevision' response: %v\n%s", panicked, debug.Stack())
					bsc.Close()
				}
			}()

			resp := outrq.Response() // blocks till reply is received

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
		}(activeSubprotocol)
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

func (bsc *BlipSyncContext) sendDelta(sender *blip.Sender, docID string, collectionIdx *int, deltaSrcRevID string, revDelta *document.RevisionDelta, seq SequenceID, resendFullRevisionFunc func() error) error {

	properties := blipRevMessageProperties(revDelta.RevisionHistory, revDelta.ToDeleted, seq)
	properties[RevMessageDeltaSrc] = deltaSrcRevID

	base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Sending rev %q %s as delta. DeltaSrc:%s", base.UD(docID), revDelta.ToRevID, deltaSrcRevID)
	return bsc.sendRevisionWithProperties(sender, docID, revDelta.ToRevID, collectionIdx, revDelta.DeltaBytes, revDelta.AttachmentStorageMeta,
		properties, seq, resendFullRevisionFunc)
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

func (bsc *BlipSyncContext) sendNoRev(sender *blip.Sender, docID, revID string, collectionIdx *int, seq SequenceID, err error) error {
	base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Sending norev %q %s due to unavailable revision: %v", base.UD(docID), revID, err)

	noRevRq := NewNoRevMessage()
	noRevRq.SetId(docID)
	noRevRq.SetRev(revID)
	noRevRq.SetCollection(collectionIdx)
	if bsc.blipContext.ActiveSubprotocol() == BlipCBMobileReplicationV2 && bsc.clientType == BLIPClientTypeSGR2 {
		noRevRq.SetSeq(seq)
	} else {
		noRevRq.SetSequence(seq)
	}

	status, reason := base.ErrorAsHTTPStatus(err)
	noRevRq.SetError(strconv.Itoa(status))

	// Add a "reason" field that gives more detailed explanation on the cause of the error.
	noRevRq.SetReason(reason)

	noRevRq.SetNoReply(true)
	if !bsc.sendBLIPMessage(sender, noRevRq.Message) {
		return ErrClosedBLIPSender
	}

	collectionCtx, err := bsc.collections.get(collectionIdx)
	if err != nil {
		return err
	}

	if collectionCtx.sgr2PushProcessedSeqCallback != nil {
		collectionCtx.sgr2PushProcessedSeqCallback(seq)
	}

	return nil
}

// Pushes a revision body to the client
func (bsc *BlipSyncContext) sendRevision(sender *blip.Sender, docID, revID string, seq SequenceID, knownRevs map[string]bool, maxHistory int, handleChangesResponseCollection *DatabaseCollectionWithUser, collectionIdx *int) error {
	rev, err := handleChangesResponseCollection.GetRev(bsc.loggingCtx, docID, revID, true, nil)
	if base.IsDocNotFoundError(err) {
		return bsc.sendNoRev(sender, docID, revID, collectionIdx, seq, err)
	} else if err != nil {
		return fmt.Errorf("failed to GetRev for doc %s with rev %s: %w", base.UD(docID).Redact(), base.MD(revID).Redact(), err)
	}

	base.TracefCtx(bsc.loggingCtx, base.KeySync, "sendRevision, rev attachments for %s/%s are %v", base.UD(docID), revID, base.UD(rev.Attachments))
	attachmentStorageMeta := document.ToAttachmentStorageMeta(rev.Attachments)
	var bodyBytes []byte
	if base.IsEnterpriseEdition() {
		// Still need to stamp _attachments into BLIP messages
		bodyBytes, err = rev.BodyBytesWith(BodyAttachments, BodyRemoved)
		if err != nil {
			return err
		}
	} else {
		bodyBytes, err = rev.BodyBytesWith(BodyAttachments, BodyRemoved)
		if err != nil {
			return bsc.sendNoRev(sender, docID, revID, collectionIdx, seq, err)
		}
	}

	history := toHistory(rev.History, knownRevs, maxHistory)
	properties := blipRevMessageProperties(history, rev.Deleted, seq)
	if base.LogDebugEnabled(base.KeySync) {
		base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Sending rev %q %s based on %d known, digests: %v", base.UD(docID), revID, len(knownRevs), digests(attachmentStorageMeta))
	}
	return bsc.sendRevisionWithProperties(sender, docID, revID, collectionIdx, bodyBytes, attachmentStorageMeta, properties, seq, nil)
}

// digests returns a slice of digest extracted from the given attachment meta.
func digests(meta []AttachmentStorageMeta) []string {
	digests := make([]string, len(meta))
	for _, m := range meta {
		digests = append(digests, m.Digest)
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
