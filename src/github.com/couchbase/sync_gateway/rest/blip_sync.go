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

type blipHandler struct {
	context *blip.Context
	sender  *blip.Sender
	db      *db.Database

	batchSize  uint
	continuous bool
}

// HTTP handler for incoming BLIP sync connection (/db/_blipsync)
func (h *handler) handleBLIPSync() error {
	bh := blipHandler{
		context: blip.NewContext(),
		db:      h.db,
	}
	bh.addHandler("getcheckpoint", bh.handleGetCheckpoint)
	bh.addHandler("setcheckpoint", bh.handleSetCheckpoint)
	bh.addHandler("getchanges", bh.handleGetChanges)
	bh.addHandler("changes", bh.handlePushedChanges)
	bh.addHandler("rev", bh.handleAddRevision)
	bh.addHandler("getattach", bh.handleGetAttachment)
	bh.context.DefaultHandler = bh.notFound
	bh.context.Logger = func(fmt string, params ...interface{}) {
		base.LogTo("BLIP", fmt, params...)
	}
	bh.context.LogMessages = base.LogKeys["BLIP+"]
	bh.context.LogFrames = base.LogKeys["BLIP++"]

	wsHandler := func(conn *websocket.Conn) {
		h.logStatus(101, "Upgraded to BLIP+WebSocket protocol")
		defer func() {
			conn.Close()
			base.LogTo("HTTP+", "#%03d:     --> BLIP+WebSocket closed", h.serialNumber)
		}()
		bh.context.WebSocketHandler()(conn)
	}
	server := websocket.Server{
		Handshake: func(*websocket.Config, *http.Request) error { return nil },
		Handler:   wsHandler,
	}
	server.ServeHTTP(h.response, h.rq)
	return nil
}

func (bh *blipHandler) addHandler(profile string, handler func(*blip.Message) error) {
	bh.context.HandlerForProfile[profile] = func(rq *blip.Message) {
		bh.sender = rq.Sender
		base.LogTo("Sync", "%s", profile)
		if err := handler(rq); err != nil {
			status, msg := base.ErrorAsHTTPStatus(err)
			if response := rq.Response(); response != nil {
				response.SetError("HTTP", status, msg)
			}
			base.LogTo("Sync", "    --> %d %s", status, msg)
		}
	}
}

func (bh *blipHandler) notFound(rq *blip.Message) {
	base.LogTo("Sync", "%s --> 404 Unknown profile", rq.Profile())
	blip.Unhandled(rq)
}

//////// CHECKPOINTS

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
		base.LogTo("Sync", "    Sending %d changes", len(changes))
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
			go bh.handleChangesResponse(rq.Response(), changeArray)
		} else {
			rq.SetNoReply(true)
		}
		return nil
	})
}

func (bh *blipHandler) handleChangesResponse(response *blip.Message, changeArray [][]string) {
	var answer []interface{}
	if err := response.ReadJSONBody(&answer); err != nil {
		base.LogTo("Sync", "Invalid response to 'changes' message")
		return
	}
	// Response is an array where each item is either an array of known rev IDs, or a non-array
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

func (bh *blipHandler) handleAddRevision(rq *blip.Message) error {
	var body db.Body
	if err := rq.ReadJSONBody(&body); err != nil {
		return err
	}
	docID, found := body["_id"].(string)
	if !found {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing doc _id")
	}
	history := strings.Split(rq.Properties["history"], ",")
	err := bh.db.PutExistingRev(docID, body, history)
	if err == nil {
		base.LogTo("Sync", "Inserted rev %q %s", docID, body["_rev"])
	}
	return err
}

func (bh *blipHandler) handleGetAttachment(rq *blip.Message) error {
	digest := rq.Properties["digest"]
	if digest == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing 'digest'")
	}
	attachment, err := bh.db.GetAttachment(db.AttachmentKey(digest))
	if err != nil {
		return err
	}
	response := rq.Response()
	response.SetBody(attachment)
	return nil
}
