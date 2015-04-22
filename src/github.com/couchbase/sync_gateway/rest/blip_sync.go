package rest

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/snej/go-blip"
	"golang.org/x/net/websocket"

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
	username           string
	batchSize          uint
	continuous         bool
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
	ctx := blipSyncContext{
		blipContext: blip.NewContext(),
		dbc:         h.db.DatabaseContext,
		username:    h.user.Name(),
	}
	ctx.blipContext.DefaultHandler = ctx.notFound
	for profile, handlerFn := range kHandlersByProfile {
		ctx.register(profile, handlerFn)
	}

	ctx.blipContext.Logger = func(fmt string, params ...interface{}) {
		base.LogTo("BLIP", fmt, params...)
	}
	ctx.blipContext.LogMessages = base.LogKeys["BLIP+"]
	ctx.blipContext.LogFrames = base.LogKeys["BLIP++"]

	// Start a WebSocket client and connect it to the BLIP handler:
	wsHandler := func(conn *websocket.Conn) {
		h.logStatus(101, "Upgraded to BLIP+WebSocket protocol")
		defer func() {
			conn.Close()
			base.LogTo("HTTP", "#%03d:     --> BLIP+WebSocket closed", h.serialNumber)
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

		// Look up the User from the name -- necessary because (a) the user's properties may have
		// changed, and (b) User objects aren't thread-safe so each handler needs its own instance.
		user, err := ctx.dbc.Authenticator().GetUser(ctx.username)
		if err != nil {
			// Apparently the user account was deleted?
			base.LogTo("Sync", "%s    --> can't find user: %s", rq, err)
			rq.Sender.Close()
		}
		db, _ := db.GetDatabase(ctx.dbc, user)
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
	value.FixJSONNumbers()
	response.SetJSONBody(value)
	return nil
}

// Received a "setCheckpoint" request
func (bh *blipHandler) handleSetCheckpoint(rq *blip.Message) error {
	docID := "checkpoint/" + rq.Properties["client"]

	var checkpoint db.Body
	err := rq.ReadJSONBody(&checkpoint)
	if err == nil {
		_, err = bh.db.PutSpecial("local", docID, checkpoint)
	}
	return err
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
	bh.batchSize = uint(getRestrictedIntFromString(rq.Properties["batch"], 200, 10, math.MaxUint64))
	bh.continuous = false
	if val, found := rq.Properties["continuous"]; found && val != "false" {
		bh.continuous = true
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
		Continuous: bh.continuous,
		Terminator: make(chan bool),
	}
	defer close(options.Terminator)

	channelSet := channels.SetOf(channels.AllChannelWildcard)

	generateContinuousChanges(bh.db, channelSet, options, func(changes []*db.ChangeEntry) error {
		base.LogTo("Sync+", "    Sending %d changes", len(changes))
		outrq := blip.NewRequest()
		outrq.SetProfile("changes")

		changeArray := make([][]interface{}, 0, len(changes))
		for _, change := range changes {
			for _, item := range change.Changes {
				changeArray = append(changeArray, []interface{}{change.Seq, change.ID, item["rev"]})
			}
		}
		outrq.SetJSONBody(changeArray)
		if len(changeArray) > 0 {
			// Spawn a goroutine to await the client's response:
			bh.sender.Send(outrq)
			go bh.handleChangesResponse(outrq.Response(), changeArray)
		} else {
			outrq.SetNoReply(true)
			bh.sender.Send(outrq)
		}
		return nil
	})
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
		base.LogTo("Sync", "Invalid response to 'changes' message")
		return
	}
	// `answer` is an array where each item is either an array of known rev IDs, or a non-array
	// placeholder (probably 0). The item numbers match those of changeArray.
	for i, item := range answer {
		if item, ok := item.([]interface{}); ok {
			seq := changeArray[i][0].(db.SequenceID)
			docID := changeArray[i][1].(string)
			revID := changeArray[i][2].(string)
			knownRevs := make([]string, len(item))
			for i, rev := range item {
				var ok bool
				if knownRevs[i], ok = rev.(string); !ok {
					base.LogTo("Sync", "Invalid response to 'changes' message")
					return
				}
			}
			bh.sendRevision(seq, docID, revID, knownRevs)
		}
	}
}

// Handles a "changes" request, i.e. a set of changes pushed by the client
func (bh *blipHandler) handlePushedChanges(rq *blip.Message) error {
	var changeList [][]interface{}
	if err := rq.ReadJSONBody(&changeList); err != nil {
		return err
	}
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
	response.SetBody(output.Bytes())
	return nil
}

//////// DOCUMENTS:

// Pushes a revision body to the client
func (bh *blipHandler) sendRevision(seq db.SequenceID, docID string, revID string, knownRevs []string) {
	base.LogTo("Sync", "Sending rev %q %s based on %v", docID, revID, knownRevs)
	body, err := bh.db.GetRev(docID, revID, true, nil)
	if err != nil {
		base.Warn("blipHandler can't get doc %q/%s: %v", docID, revID, err)
		return
	}

	// Get the revision's history as a descending array of ancestor revIDs:
	history := db.ParseRevisions(body)[1:]
	delete(body, "_revisions")
	if len(knownRevs) > 0 {
		isKnown := map[string]bool{}
		for _, rev := range knownRevs {
			isKnown[rev] = true
		}
		for i, rev := range history {
			if isKnown[rev] {
				history = history[0 : i+1]
				break
			}
		}
	}

	outrq := blip.NewRequest()
	outrq.SetProfile("rev")
	seqJSON, _ := json.Marshal(seq)
	outrq.Properties["sequence"] = string(seqJSON)
	if len(history) > 0 {
		outrq.Properties["history"] = strings.Join(history, ",")
	}
	outrq.SetJSONBody(body)
	if atts := db.BodyAttachments(body); atts != nil {
		bh.addAllowedAttachments(atts)
		defer bh.removeAllowedAttachments(atts)
		bh.sender.Send(outrq)
		outrq.Response() // blocks till reply is received
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
	docID, found := body["_id"].(string)
	revID, rfound := body["_rev"].(string)
	if !found || !rfound {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing doc _id or _rev")
	}
	history := strings.Split(rq.Properties["history"], ",")
	base.LogTo("Sync", "Inserting rev %q %s", docID, revID)

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
	base.LogTo("Sync", "Sending attachment digest=%q (%dkb)", digest, len(attachment)/1024)
	response := rq.Response()
	response.SetBody(attachment)
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
