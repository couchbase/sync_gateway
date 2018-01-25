package rest

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"github.com/couchbase/go-blip"
	"golang.org/x/net/websocket"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"fmt"
)

// Represents one BLIP connection (socket) opened by a client.
// This connection remains open until the client closes it, and can receive any number of requests.
type blipSyncContext struct {
	blipContext        *blip.Context
	dbc                *db.DatabaseContext
	user               auth.User
	effectiveUsername  string
	batchSize          int
	continuous         bool
	activeOnly         bool
	channels           base.Set
	lock               sync.Mutex
	allowedAttachments map[string]int
}

type blipHandler struct {
	*blipSyncContext
	db *db.Database
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
	"getCheckpoint": (*blipHandler).handleGetCheckpoint,
	"setCheckpoint": (*blipHandler).handleSetCheckpoint,
	"subChanges":    userBlipHandler((*blipHandler).handleSubscribeToChanges),
	"changes":       userBlipHandler((*blipHandler).handlePushedChanges),
	"rev":           userBlipHandler((*blipHandler).handleAddRevision),
	"getAttachment": userBlipHandler((*blipHandler).handleGetAttachment),
}



// HTTP handler for incoming BLIP sync WebSocket request (/db/_blipsync)
func (h *handler) handleBLIPSync() error {
	if c := h.server.GetConfig().ReplicatorCompression; c != nil {
		blip.CompressionLevel = *c
	}

	// Create a BLIP context:
	blipContext := blip.NewContext()
	blipContext.Logger = DefaultBlipLogger
	blipContext.LogMessages = base.LogEnabledExcludingLogStar("Sync+")
	blipContext.LogFrames = base.LogEnabledExcludingLogStar("Sync++")

	// Create a BLIP-sync context and register handlers:
	ctx := blipSyncContext{
		blipContext:       blipContext,
		dbc:               h.db.DatabaseContext,
		user:              h.user,
		effectiveUsername: h.currentEffectiveUserName(),
	}
	blipContext.DefaultHandler = ctx.notFound
	for profile, handlerFn := range kHandlersByProfile {
		ctx.register(profile, handlerFn)
	}
	if !h.db.AllowConflicts() {
		ctx.register("proposeChanges", (*blipHandler).handleProposedChanges)
	}

	ctx.blipContext.FatalErrorHandler = func(err error) {
		base.LogTo("HTTP", "#%03d: [%s]    --> BLIP+WebSocket connection error: %v", blipContext.ID, h.serialNumber, err)
	}

	// Create a BLIP WebSocket handler and have it handle the request:
	server := blipContext.WebSocketServer()
	defaultHandler := server.Handler
	server.Handler = func(conn *websocket.Conn) {
		h.logStatus(101, fmt.Sprintf("[%s] Upgraded to BLIP+WebSocket protocol for user: %s.", blipContext.ID, h.effectiveUsername()))
		defer func() {
			conn.Close() // in case it wasn't closed already
			base.LogTo("HTTP+", "#%03d: [%s]    --> BLIP+WebSocket connection closed", blipContext.ID, h.serialNumber)
		}()
		defaultHandler(conn)
	}

	server.ServeHTTP(h.response, h.rq)
	return nil
}



// Registers a BLIP handler including the outer-level work of logging & error handling.
// Includes the outer handler as a nested function.
func (ctx *blipSyncContext) register(profile string, handlerFn func(*blipHandler, *blip.Message) error) {

	ctx.blipContext.HandlerForProfile[profile] = func(rq *blip.Message) {

		base.LogTo("Sync", "[%s] %s %q ... %s", ctx.blipContext.ID, rq, profile, ctx.effectiveUsername)

		db, _ := db.GetDatabase(ctx.dbc, ctx.user)
		handler := blipHandler{
			blipSyncContext: ctx,
			db:              db,
		}

		if err := handlerFn(&handler, rq); err != nil {
			status, msg := base.ErrorAsHTTPStatus(err)
			if response := rq.Response(); response != nil {
				response.SetError("HTTP", status, msg)
			}
			base.LogTo("Sync", "[%s] %s %q   --> %d %s ... %s", ctx.blipContext.ID, rq, profile, status, msg, ctx.effectiveUsername)
		} else {
			base.LogTo("Sync+", "[%s] %s %q   --> OK ... %s", ctx.blipContext.ID, rq, profile, ctx.effectiveUsername)
		}
	}
}

// Handler for unknown requests
func (ctx *blipSyncContext) notFound(rq *blip.Message) {
	base.LogTo("Sync", "[%s] %s %q ... %s", ctx.blipContext.ID, rq, rq.Profile(), ctx.effectiveUsername)
	base.LogTo("Sync", "[%s] %s    --> 404 Unknown profile ... %s", ctx.blipContext.ID, rq, ctx.effectiveUsername)
	blip.Unhandled(rq)
}

//////// CHECKPOINTS

// Received a "getCheckpoint" request
func (bh *blipHandler) handleGetCheckpoint(rq *blip.Message) error {
	docID := "checkpoint/" + rq.Properties["client"]
	response := rq.Response()
	if response == nil {
		return nil
	}

	value, err := bh.db.GetSpecial("local", docID)
	if err != nil {
		return err
	}
	if value == nil {
		return base.HTTPErrorf(401, "Not found")
	}
	response.Properties["rev"] = value["_rev"].(string)
	delete(value, "_rev")
	delete(value, "_id")
	value.FixJSONNumbers()
	response.SetJSONBody(value)
	return nil
}

// Received a "setCheckpoint" request
func (bh *blipHandler) handleSetCheckpoint(rq *blip.Message) error {
	docID := "checkpoint/" + rq.Properties["client"]

	var checkpoint db.Body
	if err := rq.ReadJSONBody(&checkpoint); err != nil {
		return err
	}
	if revID := rq.Properties["rev"]; revID != "" {
		checkpoint["_rev"] = revID
	}
	revID, err := bh.db.PutSpecial("local", docID, checkpoint)
	if err != nil {
		return err
	}
	rq.Response().Properties["rev"] = revID
	return nil
}

//////// CHANGES

// Received a "subChanges" subscription request
func (bh *blipHandler) handleSubscribeToChanges(rq *blip.Message) error {

	// Depeding on the db sequence type, use correct zero sequence for since value
	since := bh.db.CreateZeroSinceValue()

	if sinceStr, found := rq.Properties["since"]; found {
		var err error
		if since, err = db.ParseSequenceIDFromJSON([]byte(sinceStr)); err != nil {
			base.LogTo("Sync", "[%s] %s: Invalid sequence ID in 'since': %s ... %s", bh.blipContext.ID, rq, sinceStr, bh.effectiveUsername)
			since = db.SequenceID{}
		}
	}
	bh.batchSize = int(getRestrictedIntFromString(rq.Properties["batch"], 200, 10, math.MaxUint64, true))
	bh.continuous = false
	if val, found := rq.Properties["continuous"]; found && val != "false" {
		bh.continuous = true
	}
	bh.activeOnly = (rq.Properties["active_only"] == "true")
	if filter := rq.Properties["filter"]; filter == "sync_gateway/bychannel" {
		if channelsParam, found := rq.Properties["channels"]; !found {
			return base.HTTPErrorf(http.StatusBadRequest, "Missing 'channels' filter parameter")
		} else {
			channelsArray := strings.Split(channelsParam, ",")
			var err error
			bh.channels, err = channels.SetFromArray(channelsArray, channels.ExpandStar)
			if err != nil {
				return err
			} else if len(bh.channels) == 0 {
				return base.HTTPErrorf(http.StatusBadRequest, "Empty channel list")
			}
		}
	} else if filter != "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Unknown filter; try sync_gateway/bychannel")
	}
	go bh.sendChanges(rq.Sender, since)  // TODO: does this ever end?
	return nil
}

// Sends all changes since the given sequence
func (bh *blipHandler) sendChanges(sender *blip.Sender, since db.SequenceID) {
	defer func() {
		if panicked := recover(); panicked != nil {
			base.Warn("[%s] PANIC sending changes: %v\n%s", bh.blipContext.ID, panicked, debug.Stack())
		}
	}()

	base.LogTo("Sync", "[%s] Sending changes since %v ... %s", bh.blipContext.ID, since, bh.effectiveUsername)
	options := db.ChangesOptions{
		Since:      since,
		Conflicts:  true,
		Continuous: bh.continuous,
		ActiveOnly: bh.activeOnly,
		Terminator: make(chan bool),
	}
	defer close(options.Terminator)

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

	generateContinuousChanges(bh.db, channelSet, options, nil, func(changes []*db.ChangeEntry) error {
		base.LogTo("Sync+", "    [%s] Sending %d changes ... %s", bh.blipContext.ID, len(changes), bh.effectiveUsername)
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
				bh.sendBatchOfChanges(sender,nil) // Signal to client that it's caught up
			}
		}
		return nil
	})
}

func (bh *blipHandler) sendBatchOfChanges(sender *blip.Sender, changeArray [][]interface{}) {
	outrq := blip.NewRequest()
	outrq.SetProfile("changes")
	outrq.SetJSONBody(changeArray)
	if len(changeArray) > 0 {
		// Spawn a goroutine to await the client's response:
		sender.Send(outrq)
		go bh.handleChangesResponse(sender, outrq.Response(), changeArray)
	} else {
		outrq.SetNoReply(true)
		sender.Send(outrq)
	}
	if len(changeArray) > 0 {
		sequence := changeArray[0][0].(db.SequenceID)
		base.LogTo("Sync", "[%s] Sent %d changes to client, from seq %s ... %s", bh.blipContext.ID, len(changeArray), sequence.String(), bh.effectiveUsername)
	} else {
		base.LogTo("Sync", "[%s] Sent all changes to client. ... %s", bh.blipContext.ID, bh.effectiveUsername)
	}
}

// Handles the response to a pushed "changes" message, i.e. the list of revisions the client wants
func (bh *blipHandler) handleChangesResponse(sender *blip.Sender, response *blip.Message, changeArray [][]interface{}) {
	defer func() {
		if panicked := recover(); panicked != nil {
			base.Warn("PANIC handling 'changes' response: %v\n%s", panicked, debug.Stack())
		}
	}()

	var answer []interface{}
	if err := response.ReadJSONBody(&answer); err != nil {
		base.LogTo("Sync", "[%s] Invalid response to 'changes' message: %s -- %s ... %s", bh.blipContext.ID, response, err, bh.effectiveUsername)
		return
	}

	maxHistory := 0
	if max, err := strconv.ParseUint(response.Properties["maxHistory"], 10, 64); err == nil {
		maxHistory = int(max)
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
					base.LogTo("Sync", "[%s] Invalid response to 'changes' message ... %s", bh.blipContext.ID, bh.effectiveUsername)
					return
				}
			}
			bh.sendRevision(sender, seq, docID, revID, knownRevs, maxHistory)
		}
	}
}

// Handles a "changes" request, i.e. a set of changes pushed by the client
func (bh *blipHandler) handlePushedChanges(rq *blip.Message) error {
	if !bh.db.AllowConflicts() {
		return base.HTTPErrorf(http.StatusConflict, "Use 'proposeChanges' instead")
	}

	var changeList [][]interface{}
	if err := rq.ReadJSONBody(&changeList); err != nil {
		return err
	}
	base.LogTo("Sync", "[%s] Received %d changes from client ... %s", bh.blipContext.ID, len(changeList), bh.effectiveUsername)
	if len(changeList) == 0 {
		return nil
	}
	output := bytes.NewBuffer(make([]byte, 0, 100*len(changeList)))
	output.Write([]byte("["))
	jsonOutput := json.NewEncoder(output)
	nWritten := 0
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
	base.LogTo("Sync", "[%s] Received %d changes from client", bh.blipContext.ID, len(changeList))
	if len(changeList) == 0 {
		return nil
	}
	output := bytes.NewBuffer(make([]byte, 0, 5*len(changeList)))
	output.Write([]byte("["))
	nWritten := 0
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
	response.SetCompressed(true)
	response.SetBody(output.Bytes())
	return nil
}

//////// DOCUMENTS:

// Pushes a revision body to the client
func (bh *blipHandler) sendRevision(sender *blip.Sender, seq db.SequenceID, docID string, revID string, knownRevs map[string]bool, maxHistory int) {
	base.LogTo("Sync+", "[%s] Sending rev %q %s based on %d known ... %s", bh.blipContext.ID, docID, revID, len(knownRevs), bh.effectiveUsername)
	body, err := bh.db.GetRev(docID, revID, true, nil)
	if err != nil {
		base.Warn("blipHandler can't get doc %q/%s: %v", docID, revID, err)
		return
	}

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

	outrq := blip.NewRequest()
	outrq.SetProfile("rev")
	seqJSON, _ := json.Marshal(seq)
	outrq.Properties["id"] = docID
	delete(body, "_id")
	outrq.Properties["rev"] = revID
	delete(body, "_rev")
	if del, _ := body["_deleted"].(bool); del {
		outrq.Properties["deleted"] = "1"
		delete(body, "deleted")
	}
	outrq.Properties["sequence"] = string(seqJSON)
	if len(history) > 0 {
		outrq.Properties["history"] = strings.Join(history, ",")
	}
	outrq.SetJSONBody(body)
	if atts := db.BodyAttachments(body); atts != nil {
		// Allow client to download attachments in 'atts', but only while pulling this rev
		bh.addAllowedAttachments(atts)
		sender.Send(outrq)
		go func() {
			defer bh.removeAllowedAttachments(atts)
			outrq.Response() // blocks till reply is received
		}()
	} else {
		outrq.SetNoReply(true)
		sender.Send(outrq)
	}
}

// Received a "rev" request, i.e. client is pushing a revision body
func (bh *blipHandler) handleAddRevision(rq *blip.Message) error {

	var body db.Body
	if err := rq.ReadJSONBody(&body); err != nil {
		return err
	}

	// Doc metadata comes from the BLIP message metadata, not magic document properties:
	docID, found := rq.Properties["id"]
	revID, rfound := rq.Properties["rev"]
	if !found || !rfound {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing docID or revID")
	}
	if del, found := rq.Properties["deleted"]; found && del != "0" && del != "false" {
		body["_deleted"] = true // (PutExistingRev expects deleted flag in the body)
	}

	history := []string{revID}
	if historyStr := rq.Properties["history"]; historyStr != "" {
		history = append(history, strings.Split(historyStr, ",")...)
	}
	base.LogTo("Sync+", "[%s] Inserting rev %q %s history=%q, array = %#v ... %s", bh.blipContext.ID, docID, revID, rq.Properties["history"], history, bh.effectiveUsername)

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
	return bh.db.PutExistingRev(docID, body, history)
}

//////// ATTACHMENTS:

// Received a "getAttachment" request
func (bh *blipHandler) handleGetAttachment(rq *blip.Message) error {
	digest := rq.Properties["digest"]
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
	base.LogTo("Sync+", "[%s] Sending attachment with digest=%q (%dkb) ... %s", bh.blipContext.ID, digest, len(attachment)/1024, bh.effectiveUsername)
	response := rq.Response()
	response.SetBody(attachment)
	response.SetCompressed(rq.Properties["compress"] == "true")
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
				base.LogTo("Sync+", "    [%s] Verifying attachment %q (digest %s)  ... %s", bh.blipContext.ID, name, digest, bh.effectiveUsername)
				nonce, proof := db.GenerateProofOfAttachment(knownData)
				outrq := blip.NewRequest()
				outrq.Properties = map[string]string{"Profile": "proveAttachment", "digest": digest}
				outrq.SetBody(nonce)
				sender.Send(outrq)
				if body, err := outrq.Response().Body(); err != nil {
					return nil, err
				} else if string(body) != proof {
					base.LogTo("Sync+", "[%s] Error: Incorrect proof for attachment %s : I sent nonce %x, expected proof %q, got %q ... %s", bh.blipContext.ID, digest, nonce, proof, body, bh.effectiveUsername)
					return nil, base.HTTPErrorf(http.StatusForbidden, "Incorrect proof for attachment %s", digest)
				}
				return nil, nil
			} else {
				// If I don't have the attachment, I will request it from the client:
				base.LogTo("Sync+", "    [%s] Asking for attachment %q (digest %s)  ... %s", bh.blipContext.ID, name, digest, bh.effectiveUsername)
				outrq := blip.NewRequest()
				outrq.Properties = map[string]string{"Profile": "getAttachment", "digest": digest}
				if isCompressible(name, meta) {
					outrq.Properties["compress"] = "true"
				}
				sender.Send(outrq)
				return outrq.Response().Body()
			}
		})
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


func DefaultBlipLogger(eventType blip.LogEventType, fmt string, params ...interface{}) {
	switch eventType {
	case blip.LogMessage:
		base.LogTo("Sync+", fmt, params...)
	case blip.LogFrame:
		base.LogTo("Sync++", fmt, params...)
	default:
		base.LogTo("Sync", fmt, params...)
	}
}