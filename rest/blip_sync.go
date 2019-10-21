package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// Represents one BLIP connection (socket) opened by a client.
// This connection remains open until the client closes it, and can receive any number of requests.
type blipSyncContext struct {
	blipContext         *blip.Context
	db                  *db.Database
	batchSize           int
	gotSubChanges       bool
	continuous          bool
	activeOnly          bool
	channels            base.Set
	lock                sync.Mutex
	allowedAttachments  map[string]int
	handlerSerialNumber uint64    // Each handler within a context gets a unique serial number for logging
	terminatorOnce      sync.Once // Used to ensure the terminator channel below is only ever closed once.
	terminator          chan bool // Closed during blipSyncContext.close(). Ensures termination of async goroutines.
	activeSubChanges    uint32    // Flag for whether there is a subChanges subscription currently active.  Atomic access
	useDeltas           bool      // Whether deltas can be used for this connection - This should be set via setUseDeltas()
	sgCanUseDeltas      bool      // Whether deltas can be used by Sync Gateway for this connection
}

type blipHandler struct {
	*blipSyncContext
	db           *db.Database
	serialNumber uint64 // This blip handler's serial number to differentiate logs w/ other handlers
}

type blipHandlerFunc func(*blipHandler, *blip.Message) error

// userBlipHandler wraps another blip handler with code that reloads the user object to make sure that
// it has the latest channel access grants.
func userBlipHandler(next blipHandlerFunc) blipHandlerFunc {
	return func(bh *blipHandler, bm *blip.Message) error {
		// Create a user-scoped database on each blip request (otherwise runs into SG issue #2717)
		if oldUser := bh.db.User(); oldUser != nil {
			newUser, err := bh.db.Authenticator().GetUser(oldUser.Name())
			if err != nil {
				return err
			}

			newDatabase, err := db.GetDatabase(bh.db.DatabaseContext, newUser)
			if err != nil {
				return err
			}
			newDatabase.Ctx = bh.db.Ctx
			bh.db = newDatabase
		}

		// Call down to the underlying handler and return it's value
		return next(bh, bm)
	}
}

// kHandlersByProfile defines the routes for each message profile (verb) of an incoming request to the function that handles it.
var kHandlersByProfile = map[string]blipHandlerFunc{
	messageGetCheckpoint:  (*blipHandler).handleGetCheckpoint,
	messageSetCheckpoint:  (*blipHandler).handleSetCheckpoint,
	messageSubChanges:     userBlipHandler((*blipHandler).handleSubChanges),
	messageChanges:        userBlipHandler((*blipHandler).handleChanges),
	messageRev:            userBlipHandler((*blipHandler).handleRev),
	messageGetAttachment:  userBlipHandler((*blipHandler).handleGetAttachment),
	messageProposeChanges: (*blipHandler).handleProposeChanges,
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
	blipContext.LogMessages = base.LogDebugEnabled(base.KeyWebSocket)
	blipContext.LogFrames = base.LogDebugEnabled(base.KeyWebSocketFrame)

	// Overwrite the existing logging context with the blip context ID
	h.db.Ctx = context.WithValue(h.db.Ctx, base.LogContextKey{},
		base.LogContext{CorrelationID: base.FormatBlipContextID(blipContext.ID)},
	)
	blipContext.Logger = DefaultBlipLogger(h.db.Ctx)

	// Create a BLIP-sync context and register handlers:
	ctx := blipSyncContext{
		blipContext: blipContext,
		db:          h.db,
		terminator:  make(chan bool),
	}
	defer ctx.close()

	// determine if SG has delta sync enabled for the given database
	ctx.sgCanUseDeltas = ctx.db.DeltaSyncEnabled()

	blipContext.DefaultHandler = ctx.notFound
	for profile, handlerFn := range kHandlersByProfile {
		ctx.register(profile, handlerFn)
	}

	ctx.blipContext.FatalErrorHandler = func(err error) {
		ctx.Logf(base.LevelInfo, base.KeyHTTP, "%s:     --> BLIP+WebSocket connection error: %v", h.formatSerialNumber(), err)
	}

	// Create a BLIP WebSocket handler and have it handle the request:
	server := blipContext.WebSocketServer()
	defaultHandler := server.Handler
	server.Handler = func(conn *websocket.Conn) {
		h.logStatus(101, fmt.Sprintf("[%s] Upgraded to BLIP+WebSocket protocol%s", blipContext.ID, h.formattedEffectiveUserName()))
		defer func() {
			_ = conn.Close() // in case it wasn't closed already
			ctx.Logf(base.LevelInfo, base.KeyHTTP, "%s:    --> BLIP+WebSocket connection closed", h.formatSerialNumber())
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
		handler := blipHandler{
			blipSyncContext: ctx,
			db:              ctx.db,
			serialNumber:    ctx.incrementSerialNumber(),
		}

		if err := handlerFn(&handler, rq); err != nil {
			status, msg := base.ErrorAsHTTPStatus(err)
			if response := rq.Response(); response != nil {
				response.SetError("HTTP", status, msg)
			}
			ctx.Logf(base.LevelInfo, base.KeySyncMsg, "#%d: Type:%s   --> %d %s Time:%v", handler.serialNumber, profile, status, msg, time.Since(startTime))
		} else {

			// Log the fact that the handler has finished, except for the "subChanges" special case which does it's own termination related logging
			if profile != "subChanges" {
				ctx.Logf(base.LevelDebug, base.KeySyncMsg, "#%d: Type:%s   --> OK Time:%v", handler.serialNumber, profile, time.Since(startTime))
			}
		}
	}

	ctx.blipContext.HandlerForProfile[profile] = handlerFnWrapper

}

func (ctx *blipSyncContext) close() {
	if ctx.gotSubChanges {
		stat := base.StatKeyPullReplicationsActiveOneShot
		if ctx.continuous {
			stat = base.StatKeyPullReplicationsActiveContinuous
		}
		ctx.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(stat, -1)
	}

	ctx.terminatorOnce.Do(func() {
		close(ctx.terminator)
	})
}

// Handler for unknown requests
func (ctx *blipSyncContext) notFound(rq *blip.Message) {
	ctx.Logf(base.LevelInfo, base.KeySync, "%s Type:%q", rq, rq.Profile())
	ctx.Logf(base.LevelInfo, base.KeySync, "%s    --> 404 Unknown profile", rq)
	blip.Unhandled(rq)
}

func (ctx *blipSyncContext) Logf(logLevel base.LogLevel, logKey base.LogKey, format string, args ...interface{}) {
	switch logLevel {
	case base.LevelError:
		base.ErrorfCtx(ctx.db.Ctx, logKey, format, args...)
	case base.LevelWarn:
		base.WarnfCtx(ctx.db.Ctx, logKey, format, args...)
	case base.LevelInfo:
		base.InfofCtx(ctx.db.Ctx, logKey, format, args...)
	case base.LevelDebug:
		base.DebugfCtx(ctx.db.Ctx, logKey, format, args...)
	case base.LevelTrace:
		base.TracefCtx(ctx.db.Ctx, logKey, format, args...)
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
	// TODO: Marshaling here when we could use raw bytes all the way from the bucket
	_ = response.SetJSONBody(value)
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

	bh.gotSubChanges = true

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
	if bh.hasActiveSubChanges() {
		return fmt.Errorf("blipHandler already has an outstanding continous subChanges.  Cannot open another one.")
	}

	bh.setActiveSubChanges(true)

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
		// Pull replication stats by type - Active stats decremented in Close()
		if bh.continuous {
			bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyPullReplicationsActiveContinuous, 1)
			bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyPullReplicationsTotalContinuous, 1)
		} else {
			bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyPullReplicationsActiveOneShot, 1)
			bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyPullReplicationsTotalOneShot, 1)
		}

		defer func() {
			bh.setActiveSubChanges(false)
		}()
		// sendChanges runs until blip context closes, or fails due to error
		startTime := time.Now()
		bh.sendChanges(rq.Sender, subChangesParams)
		bh.Logf(base.LevelDebug, base.KeySyncMsg, "#%d: Type:%s   --> Time:%v", bh.serialNumber, rq.Profile(), time.Since(startTime))
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

	bh.Logf(base.LevelInfo, base.KeySync, "Sending changes since %v", params.since())

	options := db.ChangesOptions{
		Since:      params.since(),
		Conflicts:  false, // CBL 2.0/BLIP don't support branched rev trees (LiteCore #437)
		Continuous: bh.continuous,
		ActiveOnly: bh.activeOnly,
		Terminator: bh.blipSyncContext.terminator,
		Ctx:        bh.db.Ctx,
	}

	channelSet := bh.channels
	if channelSet == nil {
		channelSet = base.SetOf(channels.AllChannelWildcard)
	}

	caughtUp := false
	pendingChanges := make([][]interface{}, 0, bh.batchSize)
	sendPendingChangesAt := func(minChanges int) error {
		if len(pendingChanges) >= minChanges {
			if err := bh.sendBatchOfChanges(sender, pendingChanges); err != nil {
				return err
			}
			pendingChanges = make([][]interface{}, 0, bh.batchSize)
		}
		return nil
	}

	_, forceClose := generateBlipSyncChanges(bh.db, channelSet, options, params.docIDs(), func(changes []*db.ChangeEntry) error {
		bh.Logf(base.LevelDebug, base.KeySync, "    Sending %d changes", len(changes))
		for _, change := range changes {

			if !strings.HasPrefix(change.ID, "_") {
				for _, item := range change.Changes {
					changeRow := []interface{}{change.Seq, change.ID, item["rev"], change.Deleted}
					if !change.Deleted {
						changeRow = changeRow[0:3]
					}
					pendingChanges = append(pendingChanges, changeRow)
					if err := sendPendingChangesAt(bh.batchSize); err != nil {
						return err
					}
				}
			}
		}
		if caughtUp || len(changes) == 0 {
			if err := sendPendingChangesAt(1); err != nil {
				return err
			}
			if !caughtUp {
				caughtUp = true
				// Signal to client that it's caught up
				if err := bh.sendBatchOfChanges(sender, nil); err != nil {
					return err
				}
			}
		}
		return nil
	})

	// On forceClose, send notify to trigger immediate exit from change waiter
	if forceClose && bh.db.User() != nil {
		bh.db.DatabaseContext.NotifyTerminatedChanges(bh.db.User().Name())
	}

}

func (bh *blipHandler) sendBatchOfChanges(sender *blip.Sender, changeArray [][]interface{}) error {
	outrq := blip.NewRequest()
	outrq.SetProfile("changes")
	outrq.SetJSONBody(changeArray)

	if len(changeArray) > 0 {
		sendTime := time.Now()
		if !sender.Send(outrq) {
			return ErrClosedBLIPSender
		}
		// Spawn a goroutine to await the client's response:
		go func(bh *blipHandler, sender *blip.Sender, response *blip.Message, changeArray [][]interface{}, sendTime time.Time) {
			if err := bh.handleChangesResponse(sender, response, changeArray, sendTime); err != nil {
				bh.Logf(base.LevelError, base.KeyAll, "Error from bh.handleChangesResponse: %v", err)
			}
		}(bh, sender, outrq.Response(), changeArray, sendTime)
	} else {
		outrq.SetNoReply(true)
		if !sender.Send(outrq) {
			return ErrClosedBLIPSender
		}
	}

	if len(changeArray) > 0 {
		sequence := changeArray[0][0].(db.SequenceID)
		bh.Logf(base.LevelInfo, base.KeySync, "Sent %d changes to client, from seq %s", len(changeArray), sequence.String())
	} else {
		bh.Logf(base.LevelInfo, base.KeySync, "Sent all changes to client")
	}

	return nil
}

// Handles the response to a pushed "changes" message, i.e. the list of revisions the client wants
func (bh *blipHandler) handleChangesResponse(sender *blip.Sender, response *blip.Message, changeArray [][]interface{}, requestSent time.Time) error {
	defer func() {
		if panicked := recover(); panicked != nil {
			base.Warnf(base.KeyAll, "[%s] PANIC handling 'changes' response: %v\n%s", bh.blipContext.ID, panicked, debug.Stack())
		}
	}()

	if response.Type() == blip.ErrorType {
		errorBody, _ := response.Body()
		base.Infof(base.KeyAll, "[%s] Client returned error in changesResponse: %s", bh.blipContext.ID, errorBody)
		return nil
	}

	var answer []interface{}
	if err := response.ReadJSONBody(&answer); err != nil {
		body, _ := response.Body()
		bh.Logf(base.LevelError, base.KeyAll, "Invalid response to 'changes' message: %s -- %s.  Body: %s", response, err, body)
		return nil
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
		bh.setUseDeltas(clientDeltasStr == "true")
	} else {
		bh.Logf(base.LevelTrace, base.KeySync, "Client didn't specify 'deltas' property in 'changes' response. useDeltas: %v", bh.useDeltas)
	}

	// Maps docID --> a map containing true for revIDs known to the client
	knownRevsByDoc := make(map[string]map[string]bool, len(answer))

	// `answer` is an array where each item is either an array of known rev IDs, or a non-array
	// placeholder (probably 0). The item numbers match those of changeArray.
	var revSendTimeLatency int64
	var revSendCount int64
	for i, knownRevsArray := range answer {
		if knownRevsArray, ok := knownRevsArray.([]interface{}); ok {
			seq := changeArray[i][0].(db.SequenceID)
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
			if bh.useDeltas && len(knownRevsArray) > 0 {
				if revID, ok := knownRevsArray[0].(string); ok {
					deltaSrcRevID = revID
				}
			}

			for _, rev := range knownRevsArray {
				if revID, ok := rev.(string); ok {
					knownRevs[revID] = true
				} else {
					bh.Logf(base.LevelError, base.KeyAll, "Invalid response to 'changes' message")
					return nil
				}
			}

			var err error
			if deltaSrcRevID != "" {
				err = bh.sendRevAsDelta(sender, docID, revID, deltaSrcRevID, seq, knownRevs, maxHistory)
			} else {
				err = bh.sendRevision(sender, docID, revID, seq, knownRevs, maxHistory)
			}
			if err != nil {
				return err
			}

			revSendTimeLatency += time.Since(changesResponseReceived).Nanoseconds()
			revSendCount++
		}
	}

	if revSendCount > 0 {
		bh.db.DbStats.StatsCblReplicationPull().Add(base.StatKeyRevSendCount, revSendCount)
		bh.db.DbStats.StatsCblReplicationPull().Add(base.StatKeyRevSendLatency, revSendTimeLatency)
		bh.db.DbStats.StatsCblReplicationPull().Add(base.StatKeyRevProcessingTime, time.Since(changesResponseReceived).Nanoseconds())
	}

	return nil
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
	jsonOutput := base.JSONEncoder(output)
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
func (bh *blipHandler) handleProposeChanges(rq *blip.Message) error {
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
		bh.Logf(base.LevelDebug, base.KeyAll, "Setting deltas=true property on proposeChanges response")
		response.Properties[changesResponseDeltas] = "true"
	}
	response.SetCompressed(true)
	response.SetBody(output.Bytes())
	return nil
}

//////// DOCUMENTS:

func (bh *blipHandler) sendRevAsDelta(sender *blip.Sender, docID, revID, deltaSrcRevID string, seq db.SequenceID, knownRevs map[string]bool, maxHistory int) error {

	bh.db.DbStats.StatsDeltaSync().Add(base.StatKeyDeltasRequested, 1)

	revDelta, redactedRev, err := bh.db.GetDelta(docID, deltaSrcRevID, revID)
	if err == db.ErrForbidden {
		return err
	} else if err != nil {
		bh.Logf(base.LevelInfo, base.KeySync, "DELTA: error generating delta from %s to %s for key %s; falling back to full body replication.  err: %v", deltaSrcRevID, revID, base.UD(docID), err)
		return bh.sendRevision(sender, docID, revID, seq, knownRevs, maxHistory)
	}

	if redactedRev != nil {
		history := toHistory(redactedRev.History, knownRevs, maxHistory)
		properties := blipRevMessageProperties(history, redactedRev.Deleted, seq)
		return bh.sendRevisionWithProperties(sender, docID, revID, redactedRev.BodyBytes, nil, properties)
	}

	if revDelta == nil {
		bh.Logf(base.LevelDebug, base.KeySync, "DELTA: unable to generate delta from %s to %s for key %s; falling back to full body replication.", deltaSrcRevID, revID, base.UD(docID))
		return bh.sendRevision(sender, docID, revID, seq, knownRevs, maxHistory)
	}

	bh.Logf(base.LevelTrace, base.KeySync, "docID: %s - delta: %v", base.UD(docID), base.UD(string(revDelta.DeltaBytes)))
	if err := bh.sendDelta(sender, docID, deltaSrcRevID, revDelta, seq); err != nil {
		return err
	}

	bh.db.DbStats.StatsDeltaSync().Add(base.StatKeyDeltasSent, 1)

	return nil
}

func (bh *blipHandler) sendDelta(sender *blip.Sender, docID, deltaSrcRevID string, revDelta *db.RevisionDelta, seq db.SequenceID) error {

	properties := blipRevMessageProperties(revDelta.RevisionHistory, revDelta.ToDeleted, seq)
	properties[revMessageDeltaSrc] = deltaSrcRevID

	bh.Logf(base.LevelDebug, base.KeySync, "Sending rev %q %s as delta. DeltaSrc:%s", base.UD(docID), revDelta.ToRevID, deltaSrcRevID)
	return bh.sendRevisionWithProperties(sender, docID, revDelta.ToRevID, revDelta.DeltaBytes, revDelta.AttachmentDigests, properties)
}

func (bh *blipHandler) sendNoRev(sender *blip.Sender, docID, revID string, err error) error {

	bh.Logf(base.LevelDebug, base.KeySync, "Sending norev %q %s due to unavailable revision: %v", base.UD(docID), revID, err)

	noRevRq := NewNoRevMessage()
	noRevRq.setId(docID)
	noRevRq.setRev(revID)

	status, reason := base.ErrorAsHTTPStatus(err)
	noRevRq.setError(strconv.Itoa(status))

	// Add a "reason" field that gives more detailed explanation on the cause of the error.
	noRevRq.setReason(reason)

	noRevRq.SetNoReply(true)
	if !sender.Send(noRevRq.Message) {
		return ErrClosedBLIPSender
	}

	return nil
}

// Pushes a revision body to the client
func (bh *blipHandler) sendRevision(sender *blip.Sender, docID, revID string, seq db.SequenceID, knownRevs map[string]bool, maxHistory int) error {
	rev, err := bh.db.GetRev(docID, revID, true, nil)
	if err != nil {
		return bh.sendNoRev(sender, docID, revID, err)
	}

	// Still need to stamp _attachments into BLIP messages
	bodyBytes := rev.BodyBytes
	if len(rev.Attachments) > 0 {
		bodyBytes, err = base.InjectJSONProperties(rev.BodyBytes, base.KVPair{Key: db.BodyAttachments, Val: rev.Attachments})
		if err != nil {
			return err
		}
	}

	bh.Logf(base.LevelDebug, base.KeySync, "Sending rev %q %s based on %d known", base.UD(docID), revID, len(knownRevs))

	history := toHistory(rev.History, knownRevs, maxHistory)
	properties := blipRevMessageProperties(history, rev.Deleted, seq)
	attDigests := db.AttachmentDigests(rev.Attachments)
	return bh.sendRevisionWithProperties(sender, docID, revID, bodyBytes, attDigests, properties)
}

func toHistory(revisions db.Revisions, knownRevs map[string]bool, maxHistory int) []string {
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

// blipRevMessageProperties returns a set of BLIP message properties for the given parameters.
func blipRevMessageProperties(revisionHistory []string, deleted bool, seq db.SequenceID) blip.Properties {
	properties := make(blip.Properties)

	// TODO: Assert? db.SequenceID.MarshalJSON can never error
	seqJSON, _ := base.JSONMarshal(seq)
	properties[revMessageSequence] = string(seqJSON)

	if len(revisionHistory) > 0 {
		properties[revMessageHistory] = strings.Join(revisionHistory, ",")
	}

	if deleted {
		properties[revMessageDeleted] = "1"
	}

	return properties
}

// Pushes a revision body to the client
func (bh *blipHandler) sendRevisionWithProperties(sender *blip.Sender, docID string, revID string, bodyBytes []byte, attDigests []string, properties blip.Properties) error {

	outrq := NewRevMessage()
	outrq.setId(docID)
	outrq.setRev(revID)

	// add additional properties passed through
	outrq.setProperties(properties)

	outrq.SetJSONBodyAsBytes(bodyBytes)

	// Update read stats
	if messageBody, err := outrq.Body(); err == nil {
		bh.db.DbStats.StatsDatabase().Add(base.StatKeyDocReadsBytesBlip, int64(len(messageBody)))
	}
	bh.db.DbStats.StatsDatabase().Add(base.StatKeyNumDocReadsBlip, 1)

	if len(attDigests) > 0 {
		// Allow client to download attachments in 'atts', but only while pulling this rev
		bh.addAllowedAttachments(attDigests)
		if !sender.Send(outrq.Message) {
			return ErrClosedBLIPSender
		}
		go func() {
			defer func() {
				if panicked := recover(); panicked != nil {
					base.Warnf(base.KeyAll, "[%s] PANIC handling 'sendRevision' response: %v\n%s", bh.blipContext.ID, panicked, debug.Stack())
					bh.close()
				}
			}()
			defer bh.removeAllowedAttachments(attDigests)
			outrq.Response() // blocks till reply is received
		}()
	} else {
		outrq.SetNoReply(true)
		if !sender.Send(outrq.Message) {
			return ErrClosedBLIPSender
		}
	}

	if response := outrq.Response(); response != nil {
		if response.Type() == blip.ErrorType {
			errorBody, _ := response.Body()
			bh.Logf(base.LevelWarn, base.KeyAll, "Client returned error in rev response for doc %q / %q: %s", docID, revID, errorBody)
		}
	}

	return nil
}

// Received a "rev" request, i.e. client is pushing a revision body
func (bh *blipHandler) handleRev(rq *blip.Message) error {
	startTime := time.Now()
	defer func() {
		bh.db.DbStats.CblReplicationPush().Add(base.StatKeyWriteProcessingTime, time.Since(startTime).Nanoseconds())
	}()

	//addRevisionParams := newAddRevisionParams(rq)
	revMessage := revMessage{Message: rq}

	bh.Logf(base.LevelDebug, base.KeySyncMsg, "#%d: Type:%s %s", bh.serialNumber, rq.Profile(), revMessage.String())

	bodyBytes, err := rq.Body()
	if err != nil {
		return err
	}

	bh.db.DbStats.StatsDatabase().Add(base.StatKeyDocWritesBytesBlip, int64(len(bodyBytes)))

	// Doc metadata comes from the BLIP message metadata, not magic document properties:
	docID, found := revMessage.id()
	revID, rfound := revMessage.rev()
	if !found || !rfound {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing docID or revID")
	}

	newDoc := &db.Document{
		ID:    docID,
		RevID: revID,
	}
	newDoc.UpdateBodyBytes(bodyBytes)

	if deltaSrcRevID, isDelta := revMessage.deltaSrc(); isDelta {
		if !bh.sgCanUseDeltas {
			return base.HTTPErrorf(http.StatusBadRequest, "Deltas are disabled for this peer")
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
		deltaSrcRev, err := bh.db.GetRev(docID, deltaSrcRevID, false, nil)
		if err != nil {
			return base.HTTPErrorf(http.StatusNotFound, "Can't fetch doc for deltaSrc=%s %v", deltaSrcRevID, err)
		}

		deltaSrcBody, err := deltaSrcRev.MutableBody()
		if err != nil {
			return base.HTTPErrorf(http.StatusInternalServerError, "Unable to marshal mutable body for deltaSrc=%s %v", deltaSrcRevID, err)
		}

		// Stamp attachments so we can patch them
		if len(deltaSrcRev.Attachments) > 0 {
			deltaSrcBody[db.BodyAttachments] = map[string]interface{}(deltaSrcRev.Attachments)
		}

		deltaSrcMap := map[string]interface{}(deltaSrcBody)
		err = base.Patch(&deltaSrcMap, newDoc.Body())
		if err != nil {
			return base.HTTPErrorf(http.StatusInternalServerError, "Error patching deltaSrc with delta: %s", err)
		}

		newDoc.UpdateBody(deltaSrcMap)
		bh.Logf(base.LevelTrace, base.KeySync, "docID: %s - body after patching: %v", base.UD(docID), base.UD(deltaSrcMap))
		bh.db.DbStats.StatsDeltaSync().Add(base.StatKeyDeltaPushDocCount, 1)
	}

	// Handle and pull out expiry
	if bytes.Contains(bodyBytes, []byte(db.BodyExpiry)) {
		body := newDoc.Body()
		expiry, err := body.ExtractExpiry()
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid expiry: %v", err)
		}
		newDoc.DocExpiry = expiry
		newDoc.UpdateBody(body)
	}

	newDoc.Deleted = revMessage.deleted()

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

	// Pull out attachments
	if bytes.Contains(bodyBytes, []byte(db.BodyAttachments)) {
		body := newDoc.Body()

		// Check for any attachments I don't have yet, and request them:
		if err := bh.downloadOrVerifyAttachments(rq.Sender, body, minRevpos); err != nil {
			return err
		}

		newDoc.DocAttachments = db.GetBodyAttachments(body)
		delete(body, db.BodyAttachments)
		newDoc.UpdateBody(body)
	}

	// Finally, save the revision (with the new attachments inline)
	bh.db.DbStats.CblReplicationPush().Add(base.StatKeyDocPushCount, 1)

	_, _, err = bh.db.PutExistingRev(newDoc, history, noConflicts)

	return err
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
	bh.Logf(base.LevelDebug, base.KeySync, "Sending attachment with digest=%q (%dkb)", digest, len(attachment)/1024)
	response := rq.Response()
	response.SetBody(attachment)
	response.SetCompressed(rq.Properties[blipCompress] == "true")
	bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyAttachmentPullCount, 1)
	bh.db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyAttachmentPullBytes, int64(len(attachment)))

	return nil
}

// For each attachment in the revision, makes sure it's in the database, asking the client to
// upload it if necessary. This method blocks until all the attachments have been processed.
func (bh *blipHandler) downloadOrVerifyAttachments(sender *blip.Sender, body db.Body, minRevpos int) error {
	return bh.db.ForEachStubAttachment(body, minRevpos,
		func(name string, digest string, knownData []byte, meta map[string]interface{}) ([]byte, error) {
			if knownData != nil {
				// If I have the attachment already I don't need the client to send it, but for
				// security purposes I do need the client to _prove_ it has the data, otherwise if
				// it knew the digest it could acquire the data by uploading a document with the
				// claimed attachment, then downloading it.
				bh.Logf(base.LevelDebug, base.KeySync, "    Verifying attachment %q (digest %s)", base.UD(name), digest)
				nonce, proof := db.GenerateProofOfAttachment(knownData)
				outrq := blip.NewRequest()
				outrq.Properties = map[string]string{blipProfile: messageProveAttachment, proveAttachmentDigest: digest}
				outrq.SetBody(nonce)
				if !sender.Send(outrq) {
					return nil, ErrClosedBLIPSender
				}
				if body, err := outrq.Response().Body(); err != nil {
					return nil, err
				} else if string(body) != proof {
					bh.Logf(base.LevelWarn, base.KeySync, "Incorrect proof for attachment %s : I sent nonce %x, expected proof %q, got %q", digest, base.MD(nonce), base.MD(proof), base.MD(string(body)))
					return nil, base.HTTPErrorf(http.StatusForbidden, "Incorrect proof for attachment %s", digest)
				}
				return nil, nil
			} else {
				// If I don't have the attachment, I will request it from the client:
				bh.Logf(base.LevelDebug, base.KeySync, "    Asking for attachment %q (digest %s)", base.UD(name), digest)
				outrq := blip.NewRequest()
				outrq.Properties = map[string]string{blipProfile: messageGetAttachment, getAttachmentDigest: digest}
				if isCompressible(name, meta) {
					outrq.Properties[blipCompress] = "true"
				}
				if !sender.Send(outrq) {
					return nil, ErrClosedBLIPSender
				}
				attBody, err := outrq.Response().Body()
				if err != nil {
					return nil, err
				}

				lNum, metaLengthOK := meta["length"].(json.Number)
				metaLength, err := lNum.Int64()
				if err != nil {
					return nil, err
				}

				// Verify that the attachment we received matches the metadata stored in the document
				if !metaLengthOK || len(attBody) != int(metaLength) || db.Sha1DigestKey(attBody) != digest {
					return nil, base.HTTPErrorf(http.StatusBadRequest, "Incorrect data sent for attachment with digest: %s", digest)
				}

				return attBody, nil
			}
		})
}

func (ctx *blipSyncContext) incrementSerialNumber() uint64 {
	return atomic.AddUint64(&ctx.handlerSerialNumber, 1)
}

func (ctx *blipSyncContext) addAllowedAttachments(attDigests []string) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if ctx.allowedAttachments == nil {
		ctx.allowedAttachments = make(map[string]int, 100)
	}
	for _, digest := range attDigests {
		ctx.allowedAttachments[digest] = ctx.allowedAttachments[digest] + 1
	}
}

func (ctx *blipSyncContext) removeAllowedAttachments(attDigests []string) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	for _, digest := range attDigests {
		if n := ctx.allowedAttachments[digest]; n > 1 {
			ctx.allowedAttachments[digest] = n - 1
		} else {
			delete(ctx.allowedAttachments, digest)
		}
	}
}

func (ctx *blipSyncContext) isAttachmentAllowed(digest string) bool {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	return ctx.allowedAttachments[digest] > 0
}

func (ctx *blipSyncContext) hasActiveSubChanges() bool {
	return atomic.LoadUint32(&ctx.activeSubChanges) == uint32(1)
}

func (ctx *blipSyncContext) setActiveSubChanges(changesActive bool) {
	if changesActive {
		atomic.StoreUint32(&ctx.activeSubChanges, uint32(1))
	} else {
		atomic.StoreUint32(&ctx.activeSubChanges, uint32(0))
	}
}

// setUseDeltas will set useDeltas on the blipSyncContext as long as both sides of the connection have it enabled.
func (ctx *blipSyncContext) setUseDeltas(clientCanUseDeltas bool) {
	// Both sides want deltas
	if ctx.sgCanUseDeltas && clientCanUseDeltas {
		if !ctx.useDeltas {
			ctx.Logf(base.LevelDebug, base.KeySync, "Enabling deltas for this replication")
			ctx.db.DbStats.StatsDeltaSync().Add(base.StatKeyDeltaPullReplicationCount, 1)
			ctx.useDeltas = true
		}
		return
	}

	// Log when the client doesn't want deltas, but we do
	if ctx.sgCanUseDeltas && !clientCanUseDeltas {
		ctx.Logf(base.LevelInfo, base.KeySync, "Disabling deltas for this replication based on client setting.")
	}
	ctx.useDeltas = false
}

func (bh *blipHandler) logEndpointEntry(profile, endpoint string) {
	bh.Logf(base.LevelInfo, base.KeySyncMsg, "#%d: Type:%s %s", bh.serialNumber, profile, endpoint)
}

func DefaultBlipLogger(ctx context.Context) blip.LogFn {
	return func(eventType blip.LogEventType, format string, params ...interface{}) {
		switch eventType {
		case blip.LogMessage:
			base.DebugfCtx(ctx, base.KeyWebSocketFrame, format, params...)
		case blip.LogFrame:
			base.DebugfCtx(ctx, base.KeyWebSocket, format, params...)
		default:
			base.InfofCtx(ctx, base.KeyWebSocket, format, params...)
		}
	}
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
