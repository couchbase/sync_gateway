package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"golang.org/x/net/websocket"
)

const (

	// Blip default vals
	BlipDefaultBatchSize = uint64(200)

	BlipMinimumBatchSize = uint64(10) // Not in the replication spec - is this required?

	// The AppProtocolId part of the BLIP websocket subprotocol.  Must match identically with the peer (typically CBLite / LiteCore).
	// At some point this will need to be able to support an array of protocols.  See go-blip/issues/27.
	BlipCBMobileReplication = "CBMobile_2"
)

// Represents one BLIP connection (socket) opened by a client.
// This connection remains open until the client closes it, and can receive any number of requests.
type blipSyncContext struct {
	blipContext         *blip.Context
	dbc                 *db.DatabaseContext
	user                auth.User
	effectiveUsername   string
	batchSize           int
	continuous          bool
	activeOnly          bool
	channels            base.Set
	lock                sync.Mutex
	allowedAttachments  map[string]int
	handlerSerialNumber uint64    // Each handler within a context gets a unique serial number for logging
	terminator          chan bool // Closed during blipSyncContext.close(). Ensures termination of async goroutines.
	hasActiveSubChanges bool      // Track whether there is a subChanges subscription currently active
	useDeltas           bool      // Whether deltas can be used for this connection - This should be set via setUseDeltas()
	sgCanUseDeltas      bool      // Whether deltas can be used by Sync Gateway for this connection
}

type blipHandler struct {
	*blipSyncContext
	db           *db.Database
	serialNumber uint64 // This blip handler's serial number to differentiate logs w/ other handlers
}

type blipHandlerMethod func(*blipHandler, *blip.Message) error

// Wrap blip handler with code that reloads the user object to make sure that
// it has the latest channel access grants.
func userBlipHandler(underlyingMethod blipHandlerMethod) blipHandlerMethod {

	wrappedBlipHandler := func(bh *blipHandler, bm *blip.Message) error {

		// Reload the user on each blip request (otherwise runs into SG issue #2717)
		if err := bh.db.ReloadUser(); err != nil {
			return err
		}

		// Call down to underlying method and return it's value
		return underlyingMethod(bh, bm)
	}

	return wrappedBlipHandler

}

// Maps the profile (verb) of an incoming request to the method that handles it.
var kHandlersByProfile = map[string]blipHandlerMethod{
	messageGetCheckpoint:  (*blipHandler).handleGetCheckpoint,
	messageSetCheckpoint:  (*blipHandler).handleSetCheckpoint,
	messageSubChanges:     userBlipHandler((*blipHandler).handleSubChanges),
	messageChanges:        userBlipHandler((*blipHandler).handleChanges),
	messageRev:            userBlipHandler((*blipHandler).handleRev),
	messageGetAttachment:  userBlipHandler((*blipHandler).handleGetAttachment),
	messageProposeChanges: (*blipHandler).handleProposedChanges,
}

// HTTP handler for incoming BLIP sync WebSocket request (/db/_blipsync)
func (h *handler) handleBLIPSync() error {

	h.db.DatabaseContext.DbStats.StatsDatabase().Add(base.StatKeyNumReplicationsActive, 1)
	h.db.DatabaseContext.DbStats.StatsDatabase().Add(base.StatKeyNumReplicationsTotal, 1)
	defer h.db.DatabaseContext.DbStats.StatsDatabase().Add(base.StatKeyNumReplicationsActive, -1)

	if c := h.server.GetConfig().ReplicatorCompression; c != nil {
		blip.CompressionLevel = *c
	}

	// Create a BLIP context:
	blipContext := blip.NewContext(BlipCBMobileReplication)
	blipContext.Logger = DefaultBlipLogger(blipContext.ID)
	blipContext.LogMessages = base.LogDebugEnabled(base.KeyWebSocket)
	blipContext.LogFrames = base.LogDebugEnabled(base.KeyWebSocketFrame)

	// Create a BLIP-sync context and register handlers:
	ctx := blipSyncContext{
		blipContext:       blipContext,
		dbc:               h.db.DatabaseContext,
		user:              h.user,
		effectiveUsername: h.currentEffectiveUserName(),
		terminator:        make(chan bool),
	}
	defer ctx.close()

	// determine if SG has delta sync enabled for the given database
	ctx.sgCanUseDeltas = ctx.dbc.DeltaSyncEnabled()

	blipContext.DefaultHandler = ctx.notFound
	for profile, handlerFn := range kHandlersByProfile {
		ctx.register(profile, handlerFn)
	}

	ctx.blipContext.FatalErrorHandler = func(err error) {
		ctx.Logf(base.LevelInfo, base.KeyHTTP, "#%03d:     --> BLIP+WebSocket connection error: %v", h.serialNumber, err)
	}

	// Create a BLIP WebSocket handler and have it handle the request:
	server := blipContext.WebSocketServer()
	defaultHandler := server.Handler
	server.Handler = func(conn *websocket.Conn) {
		h.logStatus(101, fmt.Sprintf("[%s] Upgraded to BLIP+WebSocket protocol. User:%s.", blipContext.ID, ctx.effectiveUsername))
		defer func() {
			conn.Close() // in case it wasn't closed already
			ctx.Logf(base.LevelDebug, base.KeyHTTP, "#%03d:    --> BLIP+WebSocket connection closed", h.serialNumber)
		}()
		defaultHandler(conn)
	}

	server.ServeHTTP(h.response, h.rq)
	return nil
}

// Registers a BLIP handler including the outer-level work of logging & error handling.
// Includes the outer handler as a nested function.
func (ctx *blipSyncContext) register(profile string, handlerFn func(*blipHandler, *blip.Message) error) {

	// Wrap the handler function with a function that adds handling needed by all handlers
	handlerFnWrapper := func(rq *blip.Message) {

		startTime := time.Now()

		db, _ := db.GetDatabase(ctx.dbc, ctx.user)
		handler := blipHandler{
			blipSyncContext: ctx,
			db:              db,
			serialNumber:    ctx.incrementSerialNumber(),
		}

		if err := handlerFn(&handler, rq); err != nil {
			status, msg := base.ErrorAsHTTPStatus(err)
			if response := rq.Response(); response != nil {
				response.SetError("HTTP", status, msg)
			}
			ctx.Logf(base.LevelInfo, base.KeySyncMsg, "#%d: Type:%s   --> %d %s Time:%v User:%s", handler.serialNumber, profile, status, msg, time.Since(startTime), ctx.effectiveUsername)
		} else {

			// Log the fact that the handler has finished, except for the "subChanges" special case which does it's own termination related logging
			if profile != "subChanges" {
				ctx.Logf(base.LevelDebug, base.KeySyncMsg, "#%d: Type:%s   --> OK Time:%v User:%s ", handler.serialNumber, profile, time.Since(startTime), ctx.effectiveUsername)
			}
		}
	}

	ctx.blipContext.HandlerForProfile[profile] = handlerFnWrapper

}

func (ctx *blipSyncContext) close() {
	close(ctx.terminator)
}

// Handler for unknown requests
func (ctx *blipSyncContext) notFound(rq *blip.Message) {
	ctx.Logf(base.LevelInfo, base.KeySync, "%s Type:%q User:%s", rq, rq.Profile(), ctx.effectiveUsername)
	ctx.Logf(base.LevelInfo, base.KeySync, "%s    --> 404 Unknown profile. User:%s", rq, ctx.effectiveUsername)
	blip.Unhandled(rq)
}

func (ctx *blipSyncContext) Logf(logLevel base.LogLevel, logKey base.LogKey, format string, args ...interface{}) {
	formatWithContextID, paramsWithContextID := base.PrependContextID(ctx.blipContext.ID, format, args...)
	switch logLevel {
	case base.LevelError:
		base.Errorf(logKey, formatWithContextID, paramsWithContextID...)
	case base.LevelWarn:
		base.Warnf(logKey, formatWithContextID, paramsWithContextID...)
	case base.LevelInfo:
		base.Infof(logKey, formatWithContextID, paramsWithContextID...)
	case base.LevelDebug:
		base.Debugf(logKey, formatWithContextID, paramsWithContextID...)
	case base.LevelTrace:
		base.Tracef(logKey, formatWithContextID, paramsWithContextID...)
	}
}

//////// CHECKPOINTS

// Received a "getCheckpoint" request
func (bh *blipHandler) handleGetCheckpoint(rq *blip.Message) error {

	client := rq.Properties[blipClient]
	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("Client:%s", client))

	docID := fmt.Sprintf("checkpoint/%s", client)
	response := rq.Response()
	if response == nil {
		return nil
	}

	value, err := bh.db.GetSpecial("local", docID)
	if err != nil {
		return err
	}
	if value == nil {
		return base.HTTPErrorf(http.StatusNotFound, http.StatusText(http.StatusNotFound))
	}
	response.Properties[getCheckpointResponseRev] = value[db.BodyRev].(string)
	delete(value, db.BodyRev)
	delete(value, db.BodyId)
	response.SetJSONBody(value)
	return nil
}

// Received a "setCheckpoint" request
func (bh *blipHandler) handleSetCheckpoint(rq *blip.Message) error {

	checkpointMessage := SetCheckpointMessage{rq}
	bh.logEndpointEntry(rq.Profile(), checkpointMessage.String())

	docID := fmt.Sprintf("checkpoint/%s", checkpointMessage.client())

	var checkpoint db.Body
	if err := checkpointMessage.ReadJSONBody(&checkpoint); err != nil {
		return err
	}
	if revID := checkpointMessage.rev(); revID != "" {
		checkpoint[db.BodyRev] = revID
	}
	revID, err := bh.db.PutSpecial("local", docID, checkpoint)
	if err != nil {
		return err
	}

	checkpointResponse := SetCheckpointResponse{checkpointMessage.Response()}
	checkpointResponse.setRev(revID)

	return nil
}

//////// CHANGES

// Received a "subChanges" subscription request
func (bh *blipHandler) handleSubChanges(rq *blip.Message) error {

	bh.lock.Lock()
	defer bh.lock.Unlock()

	subChangesParams, err := newSubChangesParams(
		rq,
		bh.blipSyncContext,
		bh.db.CreateZeroSinceValue(),
		bh.db.ParseSequenceID,
	)
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid subChanges parameters")
	}

	// Ensure that only _one_ subChanges subscription can be open on this blip connection at any given time.  SG #3222.
	if bh.hasActiveSubChanges {
		return fmt.Errorf("blipHandler already has an outstanding continous subChanges.  Cannot open another one.")
	}

	bh.hasActiveSubChanges = true

	if len(subChangesParams.docIDs()) > 0 && subChangesParams.continuous() {
		return base.HTTPErrorf(http.StatusBadRequest, "DocIDs filter not supported for continuous subChanges")
	}

	bh.logEndpointEntry(rq.Profile(), subChangesParams.String())

	// TODO: Do we need to store the changes-specific parameters on the blip sync context?  Seems like they only need to be passed in to sendChanges
	bh.batchSize = subChangesParams.batchSize()
	bh.continuous = subChangesParams.continuous()
	bh.activeOnly = subChangesParams.activeOnly()

	if filter := subChangesParams.filter(); filter == "sync_gateway/bychannel" {
		var err error

		bh.channels, err = subChangesParams.channelsExpandedSet()
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "%s", err)
		} else if len(bh.channels) == 0 {
			return base.HTTPErrorf(http.StatusBadRequest, "Empty channel list")

		}
	} else if filter != "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Unknown filter; try sync_gateway/bychannel")
	}

	// Start asynchronous changes goroutine
	go func() {
		// Pull replication stats by type
		if bh.continuous {
			bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyPullReplicationsActiveContinuous, 1)
			bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyPullReplicationsTotalContinuous, 1)
			defer bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyPullReplicationsActiveContinuous, -1)
		} else {
			bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyPullReplicationsActiveOneShot, 1)
			bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyPullReplicationsTotalOneShot, 1)
			defer bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyPullReplicationsActiveOneShot, -1)
		}

		defer func() {
			bh.hasActiveSubChanges = false
		}()
		// sendChanges runs until blip context closes, or fails due to error
		startTime := time.Now()
		bh.sendChanges(rq.Sender, subChangesParams)
		bh.Logf(base.LevelDebug, base.KeySyncMsg, "#%d: Type:%s   --> Time:%v User:%s ", bh.serialNumber, rq.Profile(), time.Since(startTime), base.UD(bh.effectiveUsername))
	}()

	return nil
}

// Sends all changes since the given sequence
func (bh *blipHandler) sendChanges(sender *blip.Sender, params *subChangesParams) {
	defer func() {
		if panicked := recover(); panicked != nil {
			base.Warnf(base.KeyAll, "[%s] PANIC sending changes: %v\n%s", bh.blipContext.ID, panicked, debug.Stack())
		}
	}()

	// Don't send conflicting rev tree branches, just send the winning revision + history, since
	// CBL 2.0 (and blip_sync) don't support branched revision trees.  See LiteCore #437.
	sendConflicts := false

	bh.Logf(base.LevelInfo, base.KeySync, "Sending changes since %v. User:%s", params.since(), base.UD(bh.effectiveUsername))

	options := db.ChangesOptions{
		Since:      params.since(),
		Conflicts:  sendConflicts,
		Continuous: bh.continuous,
		ActiveOnly: bh.activeOnly,
		Terminator: bh.blipSyncContext.terminator,
	}

	channelSet := bh.channels
	if channelSet == nil {
		channelSet = channels.SetOf(channels.AllChannelWildcard)
	}

	caughtUp := false
	pendingChanges := make([][]interface{}, 0, bh.batchSize)
	sendPendingChangesAt := func(minChanges int) {
		if len(pendingChanges) >= minChanges {
			bh.sendBatchOfChanges(sender, pendingChanges)
			pendingChanges = make([][]interface{}, 0, bh.batchSize)
		}
	}

	_, forceClose := generateBlipSyncChanges(bh.db, channelSet, options, params.docIDs(), func(changes []*db.ChangeEntry) error {
		bh.Logf(base.LevelDebug, base.KeySync, "    Sending %d changes. User:%s", len(changes), base.UD(bh.effectiveUsername))
		for _, change := range changes {

			if !strings.HasPrefix(change.ID, "_") {
				for _, item := range change.Changes {
					changeRow := []interface{}{change.Seq, change.ID, item["rev"], change.Deleted}
					if !change.Deleted {
						changeRow = changeRow[0:3]
					}
					pendingChanges = append(pendingChanges, changeRow)
					sendPendingChangesAt(bh.batchSize)
				}
			}
		}
		if caughtUp || len(changes) == 0 {
			sendPendingChangesAt(1)
			if !caughtUp {
				caughtUp = true
				bh.sendBatchOfChanges(sender, nil) // Signal to client that it's caught up
			}
		}
		return nil
	})

	// On forceClose, send notify to trigger immediate exit from change waiter
	if forceClose && bh.user != nil {
		bh.db.DatabaseContext.NotifyTerminatedChanges(bh.user.Name())
	}

}

func (bh *blipHandler) sendBatchOfChanges(sender *blip.Sender, changeArray [][]interface{}) {
	outrq := blip.NewRequest()
	outrq.SetProfile("changes")
	outrq.SetJSONBody(changeArray)
	if len(changeArray) > 0 {
		// Spawn a goroutine to await the client's response:
		sendTime := time.Now()
		sender.Send(outrq)
		go bh.handleChangesResponse(sender, outrq.Response(), changeArray, sendTime)
	} else {
		outrq.SetNoReply(true)
		sender.Send(outrq)
	}
	if len(changeArray) > 0 {
		sequence := changeArray[0][0].(db.SequenceID)
		bh.Logf(base.LevelInfo, base.KeySync, "Sent %d changes to client, from seq %s.  User:%s", len(changeArray), sequence.String(), base.UD(bh.effectiveUsername))
	} else {
		bh.Logf(base.LevelInfo, base.KeySync, "Sent all changes to client. User:%s", base.UD(bh.effectiveUsername))
	}
}

// Handles the response to a pushed "changes" message, i.e. the list of revisions the client wants
func (bh *blipHandler) handleChangesResponse(sender *blip.Sender, response *blip.Message, changeArray [][]interface{}, requestSent time.Time) {
	defer func() {
		if panicked := recover(); panicked != nil {
			base.Warnf(base.KeyAll, "[%s] PANIC handling 'changes' response: %v\n%s", bh.blipContext.ID, panicked, debug.Stack())
		}
	}()

	var answer []interface{}
	if err := response.ReadJSONBody(&answer); err != nil {
		bh.Logf(base.LevelInfo, base.KeySync, "Invalid response to 'changes' message: %s -- %s.  User:%s", response, err, base.UD(bh.effectiveUsername))
		return
	}
	changesResponseReceived := time.Now()

	bh.db.DbStats.StatsCblReplicationPull().Add(base.StatKeyRequestChangesCount, 1)
	bh.db.DbStats.StatsCblReplicationPull().Add(base.StatKeyRequestChangesTime, time.Since(requestSent).Nanoseconds())

	maxHistory := 0
	if max, err := strconv.ParseUint(response.Properties[changesResponseMaxHistory], 10, 64); err == nil {
		maxHistory = int(max)
	}

	// Set useDeltas if the client has delta support and has it enabled
	if clientDeltasStr, ok := response.Properties[changesResponseDeltas]; ok {
		bh.Logf(base.LevelDebug, base.KeySync, "Value for 'deltas' property in 'changes' response: %v", clientDeltasStr)
		bh.setUseDeltas(clientDeltasStr == "true")
	} else {
		bh.Logf(base.LevelDebug, base.KeySync, "No 'deltas' property found in 'changes' response.")
	}

	// Maps docID --> a map containing true for revIDs known to the client
	knownRevsByDoc := make(map[string]map[string]bool, len(answer))

	// `answer` is an array where each item is either an array of known rev IDs, or a non-array
	// placeholder (probably 0). The item numbers match those of changeArray.
	for i, knownRevsArray := range answer {
		if knownRevsArray, ok := knownRevsArray.([]interface{}); ok {
			seq := changeArray[i][0].(db.SequenceID)
			docID := changeArray[i][1].(string)
			revID := changeArray[i][2].(string)
			//deleted := changeArray[i][3].(bool)
			knownRevs := knownRevsByDoc[docID]
			if knownRevs == nil {
				knownRevs = make(map[string]bool, len(knownRevsArray))
				knownRevsByDoc[docID] = knownRevs
			}
			for _, rev := range knownRevsArray {
				if revID, ok := rev.(string); ok {
					knownRevs[revID] = true
				} else {
					bh.Logf(base.LevelInfo, base.KeySync, "Invalid response to 'changes' message.  User:%s", base.UD(bh.effectiveUsername))
					return
				}
			}

			if bh.useDeltas && len(knownRevs) > 0 {
				bh.Logf(base.LevelTrace, base.KeySync, "Attempting to send rev as delta")
				bh.sendRevAsDelta(sender, seq, docID, revID, knownRevs, maxHistory)
			} else {
				bh.Logf(base.LevelTrace, base.KeySync, "Not sending as delta because either useDeltas was false, or knownRevs was empty")
				bh.sendRevOrNorev(sender, seq, docID, revID, knownRevs, maxHistory)
			}

			bh.db.DbStats.StatsCblReplicationPull().Add(base.StatKeyRevSendCount, 1)
			bh.db.DbStats.StatsCblReplicationPull().Add(base.StatKeyRevSendTime, time.Since(changesResponseReceived).Nanoseconds())

		}
	}
}

// Handles a "changes" request, i.e. a set of changes pushed by the client
func (bh *blipHandler) handleChanges(rq *blip.Message) error {
	if !bh.db.AllowConflicts() {
		return base.HTTPErrorf(http.StatusConflict, "Use 'proposeChanges' instead")
	}
	var changeList [][]interface{}
	if err := rq.ReadJSONBody(&changeList); err != nil {
		base.Warnf(base.KeyAll, "Handle changes got error: %v", err)
		return err
	}

	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("#Changes:%d", len(changeList)))
	if len(changeList) == 0 {
		return nil
	}
	output := bytes.NewBuffer(make([]byte, 0, 100*len(changeList)))
	output.Write([]byte("["))
	jsonOutput := json.NewEncoder(output)
	nWritten := 0

	// Include changes messages w/ proposeChanges stats, although CBL should only be using proposeChanges
	startTime := time.Now()
	bh.db.DbStats.CblReplicationPush().Add(base.StatKeyProposeChangeCount, int64(len(changeList)))
	defer func() {
		bh.db.DbStats.CblReplicationPush().Add(base.StatKeyProposeChangeTime, time.Since(startTime).Nanoseconds())
	}()

	for _, change := range changeList {
		docID := change[1].(string)
		revID := change[2].(string)
		missing, possible := bh.db.RevDiff(docID, []string{revID})
		if nWritten > 0 {
			output.Write([]byte(","))
		}
		if missing == nil {
			output.Write([]byte("0"))
		} else if len(possible) == 0 {
			output.Write([]byte("[]"))
		} else {
			jsonOutput.Encode(possible)
		}
		nWritten++
	}
	output.Write([]byte("]"))
	response := rq.Response()
	response.SetCompressed(true)
	response.SetBody(output.Bytes())
	return nil
}

// Handles a "proposeChanges" request, similar to "changes" but in no-conflicts mode
func (bh *blipHandler) handleProposedChanges(rq *blip.Message) error {
	var changeList [][]interface{}
	if err := rq.ReadJSONBody(&changeList); err != nil {
		return err
	}
	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("#Changes: %d", len(changeList)))
	if len(changeList) == 0 {
		return nil
	}
	output := bytes.NewBuffer(make([]byte, 0, 5*len(changeList)))
	output.Write([]byte("["))
	nWritten := 0

	// proposeChanges stats
	startTime := time.Now()
	bh.db.DbStats.CblReplicationPush().Add(base.StatKeyProposeChangeCount, int64(len(changeList)))
	defer func() {
		bh.db.DbStats.CblReplicationPush().Add(base.StatKeyProposeChangeTime, time.Since(startTime).Nanoseconds())
	}()

	for i, change := range changeList {
		docID := change[0].(string)
		revID := change[1].(string)
		parentRevID := ""
		if len(change) > 2 {
			parentRevID = change[2].(string)
		}
		status := bh.db.CheckProposedRev(docID, revID, parentRevID)
		if status != 0 {
			// Skip writing trailing zeroes; but if we write a number afterwards we have to catch up
			if nWritten > 0 {
				output.Write([]byte(","))
			}
			for ; nWritten < i; nWritten++ {
				output.Write([]byte("0,"))
			}
			output.Write([]byte(strconv.FormatInt(int64(status), 10)))
			nWritten++
		}
	}
	output.Write([]byte("]"))
	response := rq.Response()
	if bh.sgCanUseDeltas {
		response.Properties[changesResponseDeltas] = "true"
	}
	response.SetCompressed(true)
	response.SetBody(output.Bytes())
	return nil
}

//////// DOCUMENTS:

func (bh *blipHandler) sendRevAsDelta(sender *blip.Sender, seq db.SequenceID, docID string, revID string, knownRevs map[string]bool, maxHistory int) {

	bh.db.DbStats.StatsDeltaSync().Add(base.StatKeyDeltasRequested, 1)
	bh.Logf(base.LevelTrace, base.KeySync, "DELTA: getting newBody docID: %s - rev: %v", base.UD(docID), revID)
	newBody, err := bh.db.GetRev(docID, revID, true, nil)
	if err != nil {
		bh.Logf(base.LevelWarn, base.KeySync, "DELTA: couldn't get newBody - sending NoRev... err: %v", err)
		// If we can't get the new doc, we won't be able to fall back either, so NoRev with error
		bh.sendNoRev(err, sender, seq, docID, revID)
		return
	}

	// Get the client's highest known rev ID to use as deltaSrc
	bh.Logf(base.LevelTrace, base.KeySync, "DELTA: Figuring out deltaSrcRevID from client's highest known revID...")
	var deltaSrcRevID string
	newBodyRevisions, ok := newBody[db.BodyRevisions].(db.Revisions)
	if !ok {
		bh.Logf(base.LevelWarn, base.KeySync, "DELTA: couldn't get newBody[_revisions] - sending NoRev... err: %v", err)
		// If we can't get ancestor revisions, fall back to sending full body
		bh.sendRevOrNorev(sender, seq, docID, revID, knownRevs, maxHistory)
		return
	}

	revsStart := strconv.Itoa(newBodyRevisions[db.RevisionsStart].(int) - 1) // previous rev start
	for _, v := range newBodyRevisions[db.RevisionsIds].([]string) {
		rev := revsStart + "-" + v
		if knownRevs[rev] {
			deltaSrcRevID = rev
		}
	}

	bh.Logf(base.LevelDebug, base.KeySync, "DELTA: getting oldBody docID: %s - deltaSrcRevID: %v", base.UD(docID), deltaSrcRevID)
	oldBody, err := bh.db.GetRev(docID, deltaSrcRevID, true, nil)
	if err != nil {
		bh.Logf(base.LevelDebug, base.KeySync, "DELTA: couldn't get oldBody - falling back to full body replication... err: %v", err)
		// fall back to standard full-body replication if we can't do a delta
		bh.sendRevOrNorev(sender, seq, docID, revID, knownRevs, maxHistory)
		return
	}

	// generate delta
	delta, err := base.Diff(oldBody, newBody)
	if err != nil {
		bh.Logf(base.LevelWarn, base.KeySync, "DELTA: couldn't generate delta - falling back to full body replication... err: %v", err)
		// fall back to standard full-body replication if we can't diff
		bh.sendRevOrNorev(sender, seq, docID, revID, knownRevs, maxHistory)
		return
	}
	bh.Logf(base.LevelDebug, base.KeySync, "DELTA: generated delta: %s", base.UD(delta))

	bh.sendDelta(delta, deltaSrcRevID, sender, seq, docID, revID, knownRevs, maxHistory)
	bh.db.DbStats.StatsDeltaSync().Add(base.StatKeyDeltasSent, 1)
}

func (bh *blipHandler) sendDelta(delta []byte, deltaSrcRevID string, sender *blip.Sender, seq db.SequenceID, docID string, revID string, knownRevs map[string]bool, maxHistory int) {

	var body db.Body
	if err := body.Unmarshal(delta); err != nil {
		bh.Logf(base.LevelError, base.KeySync, "DELTA: couldn't unmarshal delta... err: %v", err)
		bh.sendNoRev(err, sender, seq, docID, revID)
		return
	}

	bh.sendRevisionWithProperties(body, sender, seq, docID, revID, knownRevs, maxHistory, blip.Properties{revMessageDeltaSrc: deltaSrcRevID})
}

func (bh *blipHandler) sendRevOrNorev(sender *blip.Sender, seq db.SequenceID, docID string, revID string, knownRevs map[string]bool, maxHistory int) {

	body, err := bh.db.GetRev(docID, revID, true, nil)
	if err != nil {
		bh.sendNoRev(err, sender, seq, docID, revID)
	} else {
		bh.sendRevision(body, sender, seq, docID, revID, knownRevs, maxHistory)
	}
}

func (bh *blipHandler) sendNoRev(err error, sender *blip.Sender, seq db.SequenceID, docID string, revID string) {

	bh.Logf(base.LevelDebug, base.KeySync, "Sending norev %q %s due to unavailable revision: %v.  User:%s", base.UD(docID), revID, err, base.UD(bh.effectiveUsername))

	noRevRq := NewNoRevMessage()
	noRevRq.setId(docID)
	noRevRq.setRev(revID)

	status, reason := base.ErrorAsHTTPStatus(err)
	noRevRq.setError(strconv.Itoa(status))

	// Add a "reason" field that gives more detailed explanation on the cause of the error.
	noRevRq.setReason(reason)

	noRevRq.SetNoReply(true)
	sender.Send(noRevRq.Message)

}

// Pushes a revision body to the client
func (bh *blipHandler) sendRevision(body db.Body, sender *blip.Sender, seq db.SequenceID, docID string, revID string, knownRevs map[string]bool, maxHistory int) {
	bh.sendRevisionWithProperties(body, sender, seq, docID, revID, knownRevs, maxHistory, nil)
}

// Pushes a revision body to the client
func (bh *blipHandler) sendRevisionWithProperties(body db.Body, sender *blip.Sender, seq db.SequenceID, docID string, revID string, knownRevs map[string]bool, maxHistory int, properties blip.Properties) {

	bh.Logf(base.LevelDebug, base.KeySync, "Sending rev %q %s based on %d known.  User:%s", base.UD(docID), revID, len(knownRevs), base.UD(bh.effectiveUsername))

	// Get the revision's history as a descending array of ancestor revIDs:
	history := db.ParseRevisions(body)[1:]
	delete(body, "_revisions")
	for i, rev := range history {
		if knownRevs[rev] || (maxHistory > 0 && i+1 >= maxHistory) {
			history = history[0 : i+1]
			break
		} else {
			knownRevs[rev] = true
		}
	}

	outrq := NewRevMessage()
	outrq.setId(docID)
	outrq.setRev(revID)
	if del, _ := body[db.BodyDeleted].(bool); del {
		outrq.setDeleted(del)
	}
	outrq.setSequence(seq)
	outrq.setHistory(history)

	// add additional properties passed through
	outrq.setProperties(properties)

	delete(body, db.BodyId)
	delete(body, db.BodyRev)
	delete(body, db.BodyDeleted)

	outrq.SetJSONBody(body)

	// Update read stats
	if messageBody, err := outrq.Body(); err != nil {
		bh.db.DbStats.StatsDatabase().Add(base.StatKeyDocReadsBytesBlip, int64(len(messageBody)))
	}
	bh.db.DbStats.StatsDatabase().Add(base.StatKeyNumDocReadsBlip, 1)

	if atts := db.GetBodyAttachments(body); atts != nil {
		// Allow client to download attachments in 'atts', but only while pulling this rev
		bh.addAllowedAttachments(atts)
		sender.Send(outrq.Message)
		go func() {
			defer func() {
				if panicked := recover(); panicked != nil {
					base.Warnf(base.KeyAll, "[%s] PANIC handling 'sendRevision' response: %v\n%s", bh.blipContext.ID, panicked, debug.Stack())
					bh.close()
				}
			}()
			defer bh.removeAllowedAttachments(atts)
			outrq.Response() // blocks till reply is received
		}()
	} else {
		outrq.SetNoReply(true)
		sender.Send(outrq.Message)
	}

}

// Received a "rev" request, i.e. client is pushing a revision body
func (bh *blipHandler) handleRev(rq *blip.Message) error {

	startTime := time.Now()
	defer func() {
		bh.db.DbStats.CblReplicationPush().Add(base.StatKeyWriteProcessingTime, time.Since(startTime).Nanoseconds())
	}()

	//addRevisionParams := newAddRevisionParams(rq)
	revMessage := revMessage{Message: rq}

	bh.logEndpointEntry(rq.Profile(), revMessage.String())

	var body db.Body
	if err := rq.ReadJSONBody(&body); err != nil {
		return err
	}

	// Doc metadata comes from the BLIP message metadata, not magic document properties:
	docID, found := revMessage.id()
	revID, rfound := revMessage.rev()
	if !found || !rfound {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing docID or revID")
	}

	if deltaSrcRevID, isDelta := revMessage.deltaSrc(); isDelta {
		bh.Logf(base.LevelDebug, base.KeySync, "docID: %s - Received rev message as delta", base.UD(docID))
		if !bh.sgCanUseDeltas {
			return base.HTTPErrorf(http.StatusBadRequest, "Deltas are disabled for this peer")
		}

		delta, err := rq.Body()
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Error getting delta body: %s", err)
		}

		//  TODO: Doing a GetRevCopy here duplicates some rev cache retrieval effort, since deltaRevSrc is always
		//        going to be the current rev (no conflicts), and PutExistingRev will need to retrieve the
		//        current rev over again.  Should push this handling down PutExistingRev and use the version
		//        returned via callback in WriteUpdate, but blocked by moving attachment metadata to a rev property first
		//        (otherwise we don't have information needed to do downloadOrVerifyAttachments below prior to PutExistingRev)

		// Note: Using GetRevCopy here, and not direct rev cache retrieval, because it's still necessary to apply access check
		//       while retrieving deltaSrcRevID.  Couchbase Lite replication guarantees client has access to deltaSrcRevID,
		//       due to no-conflict write restriction, but we still need to enforce security here to prevent leaking data about previous
		//       revisions to malicious actors (in the scenario where that user has write but not read access).
		deltaSrcBody, err := bh.db.GetRevCopy(docID, deltaSrcRevID, false, nil, db.BodyDeepCopy)
		if err != nil {
			return base.HTTPErrorf(http.StatusNotFound, "Can't fetch doc for deltaSrc=%s %v", deltaSrcRevID, err)
		}

		// Strip out _id, _rev properties inserted by GetRevCopy
		delete(deltaSrcBody, db.BodyId)
		delete(deltaSrcBody, db.BodyRev)

		deltaSrcMap := map[string]interface{}(deltaSrcBody)
		err = base.Patch(&deltaSrcMap, delta)
		if err != nil {
			return base.HTTPErrorf(http.StatusInternalServerError, "Error patching deltaSrc with delta: %s", err)
		}

		body = db.Body(deltaSrcMap)
		bh.Logf(base.LevelTrace, base.KeySync, "docID: %s - body after patching: %v", base.UD(docID), base.UD(body))
	}

	if revMessage.deleted() {
		body[db.BodyDeleted] = true
	}

	// noconflicts flag from LiteCore
	// https://github.com/couchbase/couchbase-lite-core/wiki/Replication-Protocol#rev
	var noConflicts bool
	if val, ok := rq.Properties[revMessageNoConflicts]; ok {
		var err error
		noConflicts, err = strconv.ParseBool(val)
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid value for noconflicts: %s", err)
		}
	}

	history := []string{revID}
	if historyStr := rq.Properties[revMessageHistory]; historyStr != "" {
		history = append(history, strings.Split(historyStr, ",")...)
	}

	// Look at attachments with revpos > the last common ancestor's
	minRevpos := 1
	if len(history) > 0 {
		minRevpos, _ := db.ParseRevID(history[len(history)-1])
		minRevpos++
	}

	// Check for any attachments I don't have yet, and request them:
	if err := bh.downloadOrVerifyAttachments(body, minRevpos, rq.Sender); err != nil {
		return err
	}

	// Finally, save the revision (with the new attachments inline)
	bh.db.DbStats.CblReplicationPush().Add(base.StatKeyDocPushCount, 1)
	return bh.db.PutExistingRev(docID, body, history, noConflicts)
}

//////// ATTACHMENTS:

// Received a "getAttachment" request
func (bh *blipHandler) handleGetAttachment(rq *blip.Message) error {

	getAttachmentParams := newGetAttachmentParams(rq)
	bh.logEndpointEntry(rq.Profile(), getAttachmentParams.String())

	digest := getAttachmentParams.digest()
	if digest == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing 'digest'")
	}
	if !bh.isAttachmentAllowed(digest) {
		return base.HTTPErrorf(http.StatusForbidden, "Attachment's doc not being synced")
	}
	attachment, err := bh.db.GetAttachment(db.AttachmentKey(digest))
	if err != nil {
		return err
	}
	bh.Logf(base.LevelDebug, base.KeySync, "Sending attachment with digest=%q (%dkb) User:%s", digest, len(attachment)/1024, base.UD(bh.effectiveUsername))
	response := rq.Response()
	response.SetBody(attachment)
	response.SetCompressed(rq.Properties[blipCompress] == "true")
	bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyAttachmentPullCount, 1)
	bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyAttachmentPullBytes, int64(len(attachment)))

	return nil
}

// For each attachment in the revision, makes sure it's in the database, asking the client to
// upload it if necessary. This method blocks until all the attachments have been processed.
func (bh *blipHandler) downloadOrVerifyAttachments(body db.Body, minRevpos int, sender *blip.Sender) error {
	return bh.db.ForEachStubAttachment(body, minRevpos,
		func(name string, digest string, knownData []byte, meta map[string]interface{}) ([]byte, error) {
			if knownData != nil {
				// If I have the attachment already I don't need the client to send it, but for
				// security purposes I do need the client to _prove_ it has the data, otherwise if
				// it knew the digest it could acquire the data by uploading a document with the
				// claimed attachment, then downloading it.
				bh.Logf(base.LevelDebug, base.KeySync, "    Verifying attachment %q (digest %s).  User:%s", base.UD(name), digest, base.UD(bh.effectiveUsername))
				nonce, proof := db.GenerateProofOfAttachment(knownData)
				outrq := blip.NewRequest()
				outrq.Properties = map[string]string{blipProfile: messageProveAttachment, proveAttachmentDigest: digest}
				outrq.SetBody(nonce)
				sender.Send(outrq)
				if body, err := outrq.Response().Body(); err != nil {
					return nil, err
				} else if string(body) != proof {
					bh.Logf(base.LevelDebug, base.KeySync, "Error: Incorrect proof for attachment %s : I sent nonce %x, expected proof %q, got %q.  User:%s", digest, base.MD(nonce), base.MD(proof), base.MD(body), base.UD(bh.effectiveUsername))
					return nil, base.HTTPErrorf(http.StatusForbidden, "Incorrect proof for attachment %s", digest)
				}
				return nil, nil
			} else {
				// If I don't have the attachment, I will request it from the client:
				bh.Logf(base.LevelDebug, base.KeySync, "    Asking for attachment %q (digest %s). User:%s", base.UD(name), digest, base.UD(bh.effectiveUsername))
				outrq := blip.NewRequest()
				outrq.Properties = map[string]string{blipProfile: messageGetAttachment, getAttachmentDigest: digest}
				if isCompressible(name, meta) {
					outrq.Properties[blipCompress] = "true"
				}
				sender.Send(outrq)
				return outrq.Response().Body()
			}
		})
}

func (ctx *blipSyncContext) incrementSerialNumber() uint64 {
	return atomic.AddUint64(&ctx.handlerSerialNumber, 1)
}

func (ctx *blipSyncContext) addAllowedAttachments(atts map[string]interface{}) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if ctx.allowedAttachments == nil {
		ctx.allowedAttachments = make(map[string]int, 100)
	}
	for _, meta := range atts {
		if digest, ok := meta.(map[string]interface{})["digest"].(string); ok {
			ctx.allowedAttachments[digest] = ctx.allowedAttachments[digest] + 1
		}
	}
}

func (ctx *blipSyncContext) removeAllowedAttachments(atts map[string]interface{}) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	for _, meta := range atts {
		if digest, ok := meta.(map[string]interface{})["digest"].(string); ok {
			if n := ctx.allowedAttachments[digest]; n > 1 {
				ctx.allowedAttachments[digest] = n - 1
			} else {
				delete(ctx.allowedAttachments, digest)
			}
		}
	}
}

func (ctx *blipSyncContext) isAttachmentAllowed(digest string) bool {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	return ctx.allowedAttachments[digest] > 0
}

// setUseDeltas will set useDeltas on the blipSyncContext as long as both sides of the connection have it enabled.
func (ctx *blipSyncContext) setUseDeltas(clientCanUseDeltas bool) {

	shouldUseDeltas := clientCanUseDeltas && ctx.sgCanUseDeltas
	if !ctx.useDeltas && shouldUseDeltas {
		ctx.dbc.DbStats.StatsDeltaSync().Add(base.StatKeyDeltaPullReplicationCount, 1)
	}
	ctx.useDeltas = shouldUseDeltas
}

// NOTE: This code is taken from db/attachments.go in the feature/deltas branch, as of commit
// 540b1c8. Once that branch is merged it can be replaced by a call to Attachment.Compressible().

var kCompressedTypes, kGoodTypes, kBadTypes, kBadFilenames *regexp.Regexp

func init() {
	// MIME types that explicitly indicate they're compressed:
	kCompressedTypes, _ = regexp.Compile(`(?i)\bg?zip\b`)
	// MIME types that are compressible:
	kGoodTypes, _ = regexp.Compile(`(?i)(^text)|(xml\b)|(\b(html|json|yaml)\b)`)
	// ... or generally uncompressible:
	kBadTypes, _ = regexp.Compile(`(?i)^(audio|image|video)/`)
	// An interesting type is SVG (image/svg+xml) which matches _both_! (It's compressible.)
	// See <http://www.iana.org/assignments/media-types/media-types.xhtml>

	// Filename extensions of uncompressible types:
	kBadFilenames, _ = regexp.Compile(`(?i)\.(zip|t?gz|rar|7z|jpe?g|png|gif|svgz|mp3|m4a|ogg|wav|aiff|mp4|mov|avi|theora)$`)
}

// Returns true if this attachment is worth trying to compress.
func isCompressible(filename string, meta map[string]interface{}) bool {
	if meta["encoding"] != nil {
		return false
	} else if kBadFilenames.MatchString(filename) {
		return false
	} else if mimeType, ok := meta["content_type"].(string); ok && mimeType != "" {
		return !kCompressedTypes.MatchString(mimeType) &&
			(kGoodTypes.MatchString(mimeType) ||
				!kBadTypes.MatchString(mimeType))
	}
	return true // be optimistic by default
}

func (bh *blipHandler) logEndpointEntry(profile, endpoint string) {
	bh.Logf(base.LevelInfo, base.KeySyncMsg, "#%d: Type:%s %s User:%s", bh.serialNumber, profile, endpoint, base.UD(bh.effectiveUsername))
}

func DefaultBlipLogger(contextID string) blip.LogFn {
	return func(eventType blip.LogEventType, format string, params ...interface{}) {
		formatWithContextID, paramsWithContextID := base.PrependContextID(contextID, format, params...)

		switch eventType {
		case blip.LogMessage:
			base.Debugf(base.KeyWebSocketFrame, formatWithContextID, paramsWithContextID...)
		case blip.LogFrame:
			base.Debugf(base.KeyWebSocket, formatWithContextID, paramsWithContextID...)
		default:
			base.Infof(base.KeyWebSocket, formatWithContextID, paramsWithContextID...)
		}
	}
}
