package db

import (
	"context"
	"errors"
	"io"
	"regexp"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

const (
	// Blip default vals
	BlipDefaultBatchSize = uint64(200)
	BlipMinimumBatchSize = uint64(10) // Not in the replication spec - is this required?
)

var (
	// kCompressedTypes are MIME types that explicitly indicate they're compressed:
	kCompressedTypes = regexp.MustCompile(`(?i)\bg?zip\b`)

	// kGoodTypes are MIME types that are compressible:
	kGoodTypes = regexp.MustCompile(`(?i)(^text)|(xml\b)|(\b(html|json|yaml)\b)`)

	// kBadTypes are MIME types that are generally incompressible:
	kBadTypes = regexp.MustCompile(`(?i)^(audio|image|video)/`)
	// An interesting type is SVG (image/svg+xml) which matches _both_! (It's compressible.)
	// See <http://www.iana.org/assignments/media-types/media-types.xhtml>

	// kBadFilenames are filename extensions of incompressible types:
	kBadFilenames = regexp.MustCompile(`(?i)\.(zip|t?gz|rar|7z|jpe?g|png|gif|svgz|mp3|m4a|ogg|wav|aiff|mp4|mov|avi|theora)$`)
)

var ErrClosedBLIPSender = errors.New("use of closed BLIP sender")

func NewBlipSyncContext(bc *blip.Context, db *Database, contextID string) *BlipSyncContext {
	bsc := &BlipSyncContext{
		blipContext:      bc,
		blipContextDb:    db,
		loggingCtx:       db.Ctx,
		terminator:       make(chan bool),
		userChangeWaiter: db.NewUserWaiter(),
		dbStats:          db.DatabaseContext.DbStats,
		sgCanUseDeltas:   db.DeltaSyncEnabled(),
		replicationStats: NewBlipSyncStats(),
	}
	if u := db.User(); u != nil {
		bsc.userName = u.Name()
	}

	// Register default handlers
	bc.DefaultHandler = bsc.NotFoundHandler
	bc.FatalErrorHandler = func(err error) {
		base.InfofCtx(db.Ctx, base.KeyHTTP, "%s:     --> BLIP+WebSocket connection error: %v", contextID, err)
	}

	// Register 2.x replicator handlers
	for profile, handlerFn := range kHandlersByProfile {
		bsc.register(profile, handlerFn)
	}

	return bsc
}

// BlipSyncContext represents one BLIP connection (socket) opened by a client.
// This connection remains open until the client closes it, and can receive any number of requests.
type BlipSyncContext struct {
	blipContext                      *blip.Context
	blipContextDb                    *Database       // 'master' database instance for the replication, used as source when creating handler-specific databases
	loggingCtx                       context.Context // logging context for connection
	dbUserLock                       sync.RWMutex    // Must be held when refreshing the db user
	gotSubChanges                    bool
	continuous                       bool
	lock                             sync.Mutex
	allowedAttachments               map[string]int
	handlerSerialNumber              uint64                      // Each handler within a context gets a unique serial number for logging
	terminatorOnce                   sync.Once                   // Used to ensure the terminator channel below is only ever closed once.
	terminator                       chan bool                   // Closed during BlipSyncContext.close(). Ensures termination of async goroutines.
	activeSubChanges                 base.AtomicBool             // Flag for whether there is a subChanges subscription currently active.  Atomic access
	useDeltas                        bool                        // Whether deltas can be used for this connection - This should be set via setUseDeltas()
	sgCanUseDeltas                   bool                        // Whether deltas can be used by Sync Gateway for this connection
	userChangeWaiter                 *ChangeWaiter               // Tracks whether the users/roles associated with the replication have changed
	userName                         string                      // Avoid contention on db.user during userChangeWaiter user lookup
	dbStats                          *DatabaseStats              // Direct stats access to support reloading db while stats are being updated
	postHandleRevCallback            func(remoteSeq string)      // postHandleRevCallback is called after successfully handling an incoming rev message
	postHandleChangesCallback        func(expectedSeqs []string) // postHandleChangesCallback is called after successfully handling an incoming changes message
	preSendRevisionResponseCallback  func(remoteSeq string)      // preSendRevisionResponseCallback is called after sync gateway has sent a revision, but is still awaiting an acknowledgement
	postSendRevisionResponseCallback func(remoteSeq string)      // postSendRevisionResponseCallback is called after receiving acknowledgement of a sent revision
	replicationStats                 *BlipSyncStats              // Replication stats
	purgeOnRemoval                   bool                        // Purges the document when we pull a _removed:true revision.
	conflictResolver                 ConflictResolverFunc        // Conflict resolver for active replications
	// TODO: For review, whether sendRevAllConflicts needs to be per sendChanges invocation
	sendRevNoConflicts bool // Whether to set noconflicts=true when sending revisions

}

// Registers a BLIP handler including the outer-level work of logging & error handling.
// Includes the outer handler as a nested function.
func (bsc *BlipSyncContext) register(profile string, handlerFn func(*blipHandler, *blip.Message) error) {

	// Wrap the handler function with a function that adds handling needed by all handlers
	handlerFnWrapper := func(rq *blip.Message) {

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
			base.InfofCtx(bsc.loggingCtx, base.KeySyncMsg, "#%d: Type:%s   --> %d %s Time:%v", handler.serialNumber, profile, status, msg, time.Since(startTime))
		} else {
			// Log the fact that the handler has finished, except for the "subChanges" special case which does it's own termination related logging
			if profile != "subChanges" {
				base.DebugfCtx(bsc.loggingCtx, base.KeySyncMsg, "#%d: Type:%s   --> OK Time:%v", handler.serialNumber, profile, time.Since(startTime))
			}
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
	if bsc.gotSubChanges {
		stat := base.StatKeyPullReplicationsActiveOneShot
		if bsc.continuous {
			stat = base.StatKeyPullReplicationsActiveContinuous
		}
		bsc.dbStats.StatsCblReplicationPull().Add(stat, -1)
	}

	bsc.terminatorOnce.Do(func() {
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

func (bsc *BlipSyncContext) _copyContextDatabase() *Database {
	databaseCopy, _ := GetDatabase(bsc.blipContextDb.DatabaseContext, bsc.blipContextDb.User())
	databaseCopy.Ctx = bsc.loggingCtx
	return databaseCopy
}

// Handles the response to a pushed "changes" message, i.e. the list of revisions the client wants
func (bsc *BlipSyncContext) handleChangesResponse(sender *blip.Sender, response *blip.Message, changeArray [][]interface{}, requestSent time.Time, handleChangesResponseDb *Database) error {
	defer func() {
		if panicked := recover(); panicked != nil {
			base.Warnf("[%s] PANIC handling 'changes' response: %v\n%s", bsc.blipContext.ID, panicked, debug.Stack())
		}
	}()

	if response.Type() == blip.ErrorType {
		errorBody, _ := response.Body()
		base.Infof(base.KeyAll, "[%s] Client returned error in changesResponse: %s", bsc.blipContext.ID, errorBody)
		return nil
	}

	var answer []interface{}
	if err := response.ReadJSONBody(&answer); err != nil {
		body, _ := response.Body()
		if err == io.EOF {
			base.DebugfCtx(bsc.loggingCtx, base.KeyAll, "Invalid response to 'changes' message: %s -- %s.  Body: %s", response, err, body)
		} else {
			base.ErrorfCtx(bsc.loggingCtx, "Invalid response to 'changes' message: %s -- %s.  Body: %s", response, err, body)
		}
		return nil
	}
	changesResponseReceived := time.Now()

	bsc.dbStats.StatsCblReplicationPull().Add(base.StatKeyRequestChangesCount, 1)
	bsc.dbStats.StatsCblReplicationPull().Add(base.StatKeyRequestChangesTime, time.Since(requestSent).Nanoseconds())

	maxHistory := 0
	if max, err := strconv.ParseUint(response.Properties[ChangesResponseMaxHistory], 10, 64); err == nil {
		maxHistory = int(max)
	}

	// Set useDeltas if the client has delta support and has it enabled
	if clientDeltasStr, ok := response.Properties[ChangesResponseDeltas]; ok {
		bsc.setUseDeltas(clientDeltasStr == "true")
	} else {
		base.TracefCtx(bsc.loggingCtx, base.KeySync, "Client didn't specify 'deltas' property in 'changes' response. useDeltas: %v", bsc.useDeltas)
	}

	// Maps docID --> a map containing true for revIDs known to the client
	knownRevsByDoc := make(map[string]map[string]bool, len(answer))

	// `answer` is an array where each item is either an array of known rev IDs, or a non-array
	// placeholder (probably 0). The item numbers match those of changeArray.
	var revSendTimeLatency int64
	var revSendCount int64
	for i, knownRevsArray := range answer {
		if knownRevsArray, ok := knownRevsArray.([]interface{}); ok {
			seq := changeArray[i][0].(SequenceID)
			docID := changeArray[i][1].(string)
			revID := changeArray[i][2].(string)
			deltaSrcRevID := ""
			//deleted := changeArray[i][3].(bool)
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
				err = bsc.sendRevAsDelta(sender, docID, revID, deltaSrcRevID, seq, knownRevs, maxHistory, handleChangesResponseDb)
			} else {
				err = bsc.sendRevision(sender, docID, revID, seq, knownRevs, maxHistory, handleChangesResponseDb)
			}
			if err != nil {
				return err
			}

			revSendTimeLatency += time.Since(changesResponseReceived).Nanoseconds()
			revSendCount++

			if bsc.preSendRevisionResponseCallback != nil {
				bsc.preSendRevisionResponseCallback(seq.String())
			}

			// TODO: CBG-923 - Await a successful response before calling postSendRevisionCallback
			//       This is currently not available due to the NOREPLY flag being set on rev messages.

			if bsc.postSendRevisionResponseCallback != nil {
				bsc.postSendRevisionResponseCallback(seq.String())
			}
		}
	}

	if revSendCount > 0 {
		bsc.dbStats.StatsCblReplicationPull().Add(base.StatKeyRevSendCount, revSendCount)
		bsc.dbStats.StatsCblReplicationPull().Add(base.StatKeyRevSendLatency, revSendTimeLatency)
		bsc.dbStats.StatsCblReplicationPull().Add(base.StatKeyRevProcessingTime, time.Since(changesResponseReceived).Nanoseconds())
	}

	return nil
}

// Pushes a revision body to the client
func (bsc *BlipSyncContext) sendRevisionWithProperties(sender *blip.Sender, docID string, revID string, bodyBytes []byte, attDigests []string, properties blip.Properties) error {

	outrq := NewRevMessage()
	outrq.SetID(docID)
	outrq.SetRev(revID)
	if bsc.sendRevNoConflicts {
		outrq.SetNoConflicts(true)
	}

	// add additional properties passed through
	outrq.SetProperties(properties)

	outrq.SetJSONBodyAsBytes(bodyBytes)

	// Update read stats
	if messageBody, err := outrq.Body(); err == nil {
		bsc.dbStats.StatsDatabase().Add(base.StatKeyDocReadsBytesBlip, int64(len(messageBody)))
	}
	bsc.dbStats.StatsDatabase().Add(base.StatKeyNumDocReadsBlip, 1)

	base.Tracef(base.KeySync, "Sending revision %s/%s, body:%s, properties: %v, attDigests: %v", base.UD(docID), revID, base.UD(string(bodyBytes)), base.UD(properties), attDigests)

	// TODO: When CBG-881 is implemented, update this stat and SendRevCountError based on sendRev response
	bsc.replicationStats.SendRevCount.Add(1)

	if len(attDigests) > 0 {
		// Allow client to download attachments in 'atts', but only while pulling this rev
		bsc.addAllowedAttachments(attDigests)
		if !bsc.sendBLIPMessage(sender, outrq.Message) {
			return ErrClosedBLIPSender
		}
		go func() {
			defer func() {
				if panicked := recover(); panicked != nil {
					base.Warnf("[%s] PANIC handling 'sendRevision' response: %v\n%s", bsc.blipContext.ID, panicked, debug.Stack())
					bsc.Close()
				}
			}()
			defer bsc.removeAllowedAttachments(attDigests)
			outrq.Response() // blocks till reply is received
			base.Tracef(base.KeySync, "Received response for sendRevisionWithProperties rev message %s/%s", base.UD(docID), revID)
		}()
	} else {
		outrq.SetNoReply(true)
		if !bsc.sendBLIPMessage(sender, outrq.Message) {
			return ErrClosedBLIPSender
		}
	}

	if response := outrq.Response(); response != nil {
		if response.Type() == blip.ErrorType {
			errorBody, _ := response.Body()
			base.WarnfCtx(bsc.loggingCtx, "Client returned error in rev response for doc %q / %q: %s", base.UD(docID), revID, errorBody)
		}
	}

	return nil
}

func (bsc *BlipSyncContext) isAttachmentAllowed(digest string) bool {
	bsc.lock.Lock()
	defer bsc.lock.Unlock()
	return bsc.allowedAttachments[digest] > 0
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
		bsc.dbStats.StatsDeltaSync().Add(base.StatKeyDeltaPullReplicationCount, 1)
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

func (bsc *BlipSyncContext) sendDelta(sender *blip.Sender, docID, deltaSrcRevID string, revDelta *RevisionDelta, seq SequenceID) error {

	properties := blipRevMessageProperties(revDelta.RevisionHistory, revDelta.ToDeleted, seq)
	properties[RevMessageDeltaSrc] = deltaSrcRevID

	base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Sending rev %q %s as delta. DeltaSrc:%s", base.UD(docID), revDelta.ToRevID, deltaSrcRevID)
	return bsc.sendRevisionWithProperties(sender, docID, revDelta.ToRevID, revDelta.DeltaBytes, revDelta.AttachmentDigests, properties)
}

// sendBLIPMessage is a simple wrapper around all sent BLIP messages
func (bsc *BlipSyncContext) sendBLIPMessage(sender *blip.Sender, msg *blip.Message) bool {
	if base.LogTraceEnabled(base.KeySyncMsg) {
		rqBody, _ := msg.Body()
		base.TracefCtx(bsc.loggingCtx, base.KeySyncMsg, "Send Req %s: Body: '%s' Properties: %v", msg, base.UD(rqBody), base.UD(msg.Properties))
	}
	return sender.Send(msg)
}

func (bsc *BlipSyncContext) sendNoRev(sender *blip.Sender, docID, revID string, err error) error {

	base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Sending norev %q %s due to unavailable revision: %v", base.UD(docID), revID, err)

	noRevRq := NewNoRevMessage()
	noRevRq.SetId(docID)
	noRevRq.SetRev(revID)

	status, reason := base.ErrorAsHTTPStatus(err)
	noRevRq.SetError(strconv.Itoa(status))

	// Add a "reason" field that gives more detailed explanation on the cause of the error.
	noRevRq.SetReason(reason)

	noRevRq.SetNoReply(true)
	if !bsc.sendBLIPMessage(sender, noRevRq.Message) {
		return ErrClosedBLIPSender
	}

	return nil
}

// Pushes a revision body to the client
func (bsc *BlipSyncContext) sendRevision(sender *blip.Sender, docID, revID string, seq SequenceID, knownRevs map[string]bool, maxHistory int, handleChangesResponseDb *Database) error {
	rev, err := handleChangesResponseDb.GetRev(docID, revID, true, nil)
	if err != nil {
		return bsc.sendNoRev(sender, docID, revID, err)
	}

	base.Tracef(base.KeySync, "sendRevision, rev attachments for %s/%s are %v", base.UD(docID), revID, base.UD(rev.Attachments))
	var bodyBytes []byte
	if base.IsEnterpriseEdition() {
		// Still need to stamp _attachments into BLIP messages
		if len(rev.Attachments) > 0 {
			bodyBytes, err = base.InjectJSONProperties(rev.BodyBytes, base.KVPair{Key: BodyAttachments, Val: rev.Attachments})
			if err != nil {
				return err
			}
		} else {
			bodyBytes = rev.BodyBytes
		}
	} else {
		body, err := rev.MutableBody()
		if err != nil {
			return bsc.sendNoRev(sender, docID, revID, err)
		}

		// Still need to stamp _attachments into BLIP messages
		if len(rev.Attachments) > 0 {
			body[BodyAttachments] = rev.Attachments
		}

		bodyBytes, err = base.JSONMarshalCanonical(body)
		if err != nil {
			return bsc.sendNoRev(sender, docID, revID, err)
		}
	}

	history := toHistory(rev.History, knownRevs, maxHistory)
	properties := blipRevMessageProperties(history, rev.Deleted, seq)
	attDigests := AttachmentDigests(rev.Attachments)
	base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Sending rev %q %s based on %d known, digests: %v", base.UD(docID), revID, len(knownRevs), attDigests)
	return bsc.sendRevisionWithProperties(sender, docID, revID, bodyBytes, attDigests, properties)
}

// InitializeStats must be run before replication is started - there is no synchronization on replicationStats
func (bsc *BlipSyncContext) InitializeStats(stats *BlipSyncStats) {
	bsc.replicationStats = stats
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
