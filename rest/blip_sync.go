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
)

// Represents one BLIP connection (socket) opened by a client.
// This connection remains open until the client closes it, and can receive any number of requests.
type blipSyncContext struct {
	blipContext        *blip.Context
	sender             *blip.Sender
	dbc                *db.DatabaseContext
	user               auth.User
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

// Maps the profile (verb) of an incoming request to the method that handles it.
var kHandlersByProfile = map[string]func(*blipHandler, *blip.Message) error{
	"getCheckpoint": (*blipHandler).handleGetCheckpoint,
	"setCheckpoint": (*blipHandler).handleSetCheckpoint,
	"subChanges":    (*blipHandler).handleSubscribeToChanges,
	"changes":       (*blipHandler).handlePushedChanges,
	"rev":           (*blipHandler).handleAddRevision,
	"getAttachment": (*blipHandler).handleGetAttachment,
}

// HTTP handler for incoming BLIP sync WebSocket request (/db/_blipsync)
func (h *handler) handleBLIPSync() error {
	if !h.server.GetDatabaseConfig(h.db.Name).Unsupported.Replicator2 {
		return base.HTTPErrorf(http.StatusNotFound, "feature not enabled")
	}

	ctx := blipSyncContext{
		blipContext: blip.NewContext(),
		dbc:         h.db.DatabaseContext,
		user:        h.user,
	}
	ctx.blipContext.DefaultHandler = ctx.notFound
	for profile, handlerFn := range kHandlersByProfile {
		ctx.register(profile, handlerFn)
	}

	ctx.blipContext.Logger = func(fmt string, params ...interface{}) {
		base.LogTo("BLIP", fmt, params...)
	}
	ctx.blipContext.LogMessages = base.LogEnabledExcludingLogStar("BLIP+")
	ctx.blipContext.LogFrames = base.LogEnabledExcludingLogStar("BLIP++")

	// Start a WebSocket client and connect it to the BLIP handler:
	wsHandler := func(conn *websocket.Conn) {
		h.logStatus(101, "Upgraded to BLIP+WebSocket protocol")
		defer func() {
			conn.Close()
			base.LogTo("HTTP+", "#%03d:     --> BLIP+WebSocket connection closed", h.serialNumber)
		}()
		ctx.blipContext.WebSocketHandler()(conn)
	}
	server := websocket.Server{
		Handshake: func(*websocket.Config, *http.Request) error { return nil },
		Handler:   wsHandler,
	}
	server.ServeHTTP(h.response, h.rq)
	return nil
}

// Registers a BLIP handler including the outer-level work of logging & error handling.
// Includes the outer handler as a nested function.
func (ctx *blipSyncContext) register(profile string, handlerFn func(*blipHandler, *blip.Message) error) {
	ctx.blipContext.HandlerForProfile[profile] = func(rq *blip.Message) {
		ctx.sender = rq.Sender
		base.LogTo("Sync", "%s %q", rq, profile)

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
			base.LogTo("Sync", "%s    --> %d %s", rq, status, msg)
		} else {
			base.LogTo("Sync+", "%s    --> OK", rq)
		}
	}
}

// Handler for unknown requests
func (ctx *blipSyncContext) notFound(rq *blip.Message) {
	base.LogTo("Sync", "%s %q", rq, rq.Profile())
	base.LogTo("Sync", "%s    --> 404 Unknown profile", rq)
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
	var since db.SequenceID
	if sinceStr, found := rq.Properties["since"]; found {
		var err error
		if since, err = db.ParseSequenceIDFromJSON([]byte(sinceStr)); err != nil {
			base.LogTo("Sync", "%s: Invalid sequence ID in 'since': %s", rq, sinceStr)
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
	go bh.sendChanges(since)
	return nil
}

// Sends all changes since the given sequence
func (bh *blipHandler) sendChanges(since db.SequenceID) {
	defer func() {
		if panicked := recover(); panicked != nil {
			base.Warn("*** PANIC sending changes: %v\n%s", panicked, debug.Stack())
		}
	}()

	base.LogTo("Sync", "Sending changes since %v", since)
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
			bh.sendBatchOfChanges(pendingChanges)
			pendingChanges = make([][]interface{}, 0, bh.batchSize)
		}
	}

	generateContinuousChanges(bh.db, channelSet, options, nil, func(changes []*db.ChangeEntry) error {
		base.LogTo("Sync+", "    Sending %d changes", len(changes))
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
				bh.sendBatchOfChanges(nil) // Signal to client that it's caught up
			}
		}
		return nil
	})
}

func (bh *blipHandler) sendBatchOfChanges(changeArray [][]interface{}) {
	outrq := blip.NewRequest()
	outrq.SetProfile("changes")
	outrq.SetJSONBody(changeArray)
	if len(changeArray) > 0 {
		// Spawn a goroutine to await the client's response:
		bh.sender.Send(outrq)
		go bh.handleChangesResponse(outrq.Response(), changeArray)
	} else {
		outrq.SetNoReply(true)
		bh.sender.Send(outrq)
	}
	if len(changeArray) > 0 {
		base.LogTo("Sync", "Sent %d changes to client, from seq %v", len(changeArray), changeArray[0][0])
	} else {
		base.LogTo("Sync", "Sent all changes to client.")
	}
}

// Handles the response to a pushed "changes" message, i.e. the list of revisions the client wants
func (bh *blipHandler) handleChangesResponse(response *blip.Message, changeArray [][]interface{}) {
	defer func() {
		if panicked := recover(); panicked != nil {
			base.Warn("*** PANIC handling 'changes' response: %v\n%s", panicked, debug.Stack())
		}
	}()

	var answer []interface{}
	if err := response.ReadJSONBody(&answer); err != nil {
		base.LogTo("Sync", "Invalid response to 'changes' message: %s -- %s", response, err)
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
					base.LogTo("Sync", "Invalid response to 'changes' message")
					return
				}
			}
			bh.sendRevision(seq, docID, revID, knownRevs, maxHistory)
		}
	}
}

// Handles a "changes" request, i.e. a set of changes pushed by the client
func (bh *blipHandler) handlePushedChanges(rq *blip.Message) error {
	var changeList [][]interface{}
	if err := rq.ReadJSONBody(&changeList); err != nil {
		return err
	}
	base.LogTo("Sync", "Received %d changes from client", len(changeList))
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

//////// DOCUMENTS:

// Pushes a revision body to the client
func (bh *blipHandler) sendRevision(seq db.SequenceID, docID string, revID string, knownRevs map[string]bool, maxHistory int) {
	base.LogTo("Sync+", "Sending rev %q %s based on %d known", docID, revID, len(knownRevs))
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
		bh.sender.Send(outrq)
		go func() {
			defer bh.removeAllowedAttachments(atts)
			outrq.Response() // blocks till reply is received
		}()
	} else {
		outrq.SetNoReply(true)
		bh.sender.Send(outrq)
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
	base.LogTo("Sync+", "Inserting rev %q %s history=%q, array = %#v", docID, revID, rq.Properties["history"], history)

	// Look at attachments with revpos > the last common ancestor's
	minRevpos := 1
	if len(history) > 0 {
		minRevpos, _ := db.ParseRevID(history[len(history)-1])
		minRevpos++
	}

	// Check for any attachments I don't have yet, and request them:
	if err := bh.downloadOrVerifyAttachments(body, minRevpos); err != nil {
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
	base.LogTo("Sync+", "Sending attachment with digest=%q (%dkb)", digest, len(attachment)/1024)
	response := rq.Response()
	response.SetBody(attachment)
	response.SetCompressed(rq.Properties["compress"] == "true")
	return nil
}

// For each attachment in the revision, makes sure it's in the database, asking the client to
// upload it if necessary. This method blocks until all the attachments have been processed.
func (bh *blipHandler) downloadOrVerifyAttachments(body db.Body, minRevpos int) error {
	return bh.db.ForEachStubAttachment(body, minRevpos,
		func(name string, digest string, knownData []byte, meta map[string]interface{}) ([]byte, error) {
			if knownData != nil {
				// If I have the attachment already I don't need the client to send it, but for
				// security purposes I do need the client to _prove_ it has the data, otherwise if
				// it knew the digest it could acquire the data by uploading a document with the
				// claimed attachment, then downloading it.
				base.LogTo("Sync+", "    Verifying attachment %q (digest %s)...", name, digest)
				nonce, proof := db.GenerateProofOfAttachment(knownData)
				outrq := blip.NewRequest()
				outrq.Properties = map[string]string{"Profile": "proveAttachment", "digest": digest}
				outrq.SetBody(nonce)
				bh.sender.Send(outrq)
				if body, err := outrq.Response().Body(); err != nil {
					return nil, err
				} else if string(body) != proof {
					base.LogTo("Sync+", "Error: Incorrect proof for attachment %s : I sent nonce %x, expected proof %q, got %q", digest, nonce, proof, body)
					return nil, base.HTTPErrorf(http.StatusForbidden, "Incorrect proof for attachment %s", digest)
				}
				return nil, nil
			} else {
				// If I don't have the attachment, I will request it from the client:
				base.LogTo("Sync+", "    Asking for attachment %q (digest %s)...", name, digest)
				outrq := blip.NewRequest()
				outrq.Properties = map[string]string{"Profile": "getAttachment", "digest": digest}
				if isCompressible(name, meta) {
					outrq.Properties["compress"] = "true"
				}
				bh.sender.Send(outrq)
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
