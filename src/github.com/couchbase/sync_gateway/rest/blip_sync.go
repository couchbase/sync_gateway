package rest

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"strings"

	"github.com/snej/go-blip"
	"golang.org/x/net/websocket"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

// Represents one BLIP connection (socket) opened by a client.
// This connection remains open until the client closes it, and can receive any number of requests.
type blipSyncContext struct {
	blipContext *blip.Context
	sender      *blip.Sender
	dbc         *db.DatabaseContext
	username    string

	batchSize  uint
	continuous bool
}

type blipHandler struct {
	*blipSyncContext
	db *db.Database
}

// Maps the profile (verb) of an incoming request to the method that handles it.
var kHandlersByProfile = map[string]func(*blipHandler, *blip.Message) error{
	"getcheckpoint": (*blipHandler).handleGetCheckpoint,
	"setcheckpoint": (*blipHandler).handleSetCheckpoint,
	"getchanges":    (*blipHandler).handleGetChanges,
	"changes":       (*blipHandler).handlePushedChanges,
	"rev":           (*blipHandler).handleAddRevision,
	"getattach":     (*blipHandler).handleGetAttachment,
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

// Received a "getcheckpoint" request
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

// Received a "setcheckpoint" request
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

// Received a "getchanges" subscription request
func (bh *blipHandler) handleGetChanges(rq *blip.Message) error {
	var since db.SequenceID
	if sinceStr, found := rq.Properties["since"]; found {
		var err error
		if since, err = db.ParseSequenceID(sinceStr); err != nil {
			return err
		}
	}
	bh.batchSize = uint(getRestrictedIntFromString(rq.Properties["batch"], 200, 10, math.MaxUint64))
	bh.continuous = false
	if val, found := rq.Properties["continuous"]; found && val != "false" {
		bh.continuous = true
	}
	bh.sendChanges(since)
	return nil
}

// Sends all changes since the given sequence
func (bh *blipHandler) sendChanges(since db.SequenceID) {
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
		rq := blip.NewRequest()
		rq.SetProfile("changes")

		changeArray := make([][]string, len(changes))
		for i, change := range changes {
			changeArray[i] = []string{
				change.Seq.String(),
				change.ID,
				change.Changes[0]["rev"],
			} //FIX: Add all conflicts
		}
		rq.SetJSONBody(changeArray)
		bh.sender.Send(rq)
		if len(changeArray) > 0 {
			// Spawn a goroutine to await the client's response:
			go bh.handleChangesResponse(rq.Response(), changeArray)
		} else {
			rq.SetNoReply(true)
		}
		return nil
	})
}

// Handles the response to a pushed "changes" message, i.e. the list of revisions the client wants
func (bh *blipHandler) handleChangesResponse(response *blip.Message, changeArray [][]string) {
	var answer []interface{}
	if err := response.ReadJSONBody(&answer); err != nil {
		base.LogTo("Sync", "Invalid response to 'changes' message")
		return
	}
	// `answer` is an array where each item is either an array of known rev IDs, or a non-array
	// placeholder (probably 0). The item numbers match those of changeArray.
	for i, item := range answer {
		if item, ok := item.([]interface{}); ok {
			seq := changeArray[i][0]
			docID := changeArray[i][1]
			revID := changeArray[i][2]
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
func (bh *blipHandler) sendRevision(seq string, docID string, revID string, knownRevs []string) {
	base.LogTo("Sync", "Sending rev %q %s based on %v", docID, revID, knownRevs)
	body, err := bh.db.GetRev(docID, revID, true, nil)
	if err != nil {
		base.Warn("blipHandler can't get doc %q/%s: %v", docID, revID, err)
		return
	}

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

	rq := blip.NewRequest()
	rq.SetProfile("rev")
	rq.Properties["sequence"] = seq
	if len(history) > 0 {
		rq.Properties["history"] = strings.Join(history, ",")
	}
	rq.SetJSONBody(body)
	bh.sender.Send(rq)
}

// Received a "getattach" request
func (bh *blipHandler) handleGetAttachment(rq *blip.Message) error {
	digest := rq.Properties["digest"]
	if digest == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing 'digest'")
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
	err := bh.db.ForEachStubAttachment(body, minRevpos,
		func(name string, digest string, knownData []byte, meta map[string]interface{}) ([]byte, error) {
			if knownData != nil {
				// If I have the attachment already I don't need the client to send it, but for
				// security purposes I do need the client to _prove_ it has the data, otherwise if
				// it knew the digest it could acquire the data by uploading a document with the
				// claimed attachment, then downloading it.
				base.LogTo("Sync+", "    Verifying attachment %q (digest %s)...", name, digest)
				nonce, proof := db.GenerateProofOfAttachment(knownData)
				rq := blip.NewRequest()
				rq.Properties = map[string]string{"Profile": "proveattach", "digest": digest}
				rq.SetBody(nonce)
				bh.sender.Send(rq)
				if body, err := rq.Response().Body(); err != nil {
					return nil, err
				} else if string(body) != proof {
					base.LogTo("Sync+", "Error: Incorrect proof for attachment %s : I sent nonce %x, expected proof %q, got %q", digest, nonce, proof, body)
					return nil, base.HTTPErrorf(http.StatusForbidden, "Incorrect proof for attachment %s", digest)
				}
				return nil, nil
			} else {
				// If I don't have the attachment, I will request it from the client:
				base.LogTo("Sync+", "    Asking for attachment %q (digest %s)...", name, digest)
				rq := blip.NewRequest()
				rq.Properties = map[string]string{"Profile": "getattach", "digest": digest}
				bh.sender.Send(rq)
				return rq.Response().Body()
			}
		})
	if err != nil {
		return err
	}

	// Finally, save the revision (with the new attachments inline)
	return bh.db.PutExistingRev(docID, body, history)
}
