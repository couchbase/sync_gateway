//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/websocket"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

// Minimum value of _changes?heartbeat property
const kMinHeartbeatMS = 25 * 1000

// Default value of _changes?heartbeat property
const kDefaultHeartbeatMS = 0

// Default value of _changes?timeout property
const kDefaultTimeoutMS = 5 * 60 * 1000

// Maximum value of _changes?timeout property
const kMaxTimeoutMS = 15 * 60 * 1000

func (h *handler) handleRevsDiff() error {
	var input map[string][]string
	err := h.readJSONInto(&input)
	if err != nil {
		return err
	}

	h.response.Write([]byte("{"))
	first := true
	for docid, revs := range input {
		missing, possible := h.db.RevDiff(docid, revs)
		if missing != nil {
			docOutput := map[string]interface{}{"missing": missing}
			if possible != nil {
				docOutput["possible_ancestors"] = possible
			}
			if !first {
				h.response.Write([]byte(",\n"))
			}
			first = false
			h.response.Write([]byte(fmt.Sprintf("%q:", docid)))
			h.addJSON(docOutput)
		}
	}
	h.response.Write([]byte("}"))
	return nil
}

// UpdateChangesOptionsFromQuery handles any changes POST requests that send parameters in the POST body AND in the query string.  If any parameters
// are present in the query string, they override the values sent in the body.

func (h *handler) updateChangesOptionsFromQuery(feed *string, options *db.ChangesOptions, filter *string, channelsArray []string, docIdsArray []string) (newChannelsArray []string, newDocIdsArray []string, err error) {

	if h.rq.URL.RawQuery == "" {
		return channelsArray, docIdsArray, nil
	}

	values := h.rq.URL.Query()

	if _, ok := values["feed"]; ok {
		*feed = h.getQuery("feed")
	}

	if _, ok := values["since"]; ok {
		if options.Since, err = h.db.ParseSequenceID(h.getJSONStringQuery("since")); err != nil {
			return nil, nil, err
		}
	}

	if _, ok := values["limit"]; ok {
		options.Limit = int(h.getIntQuery("limit", 0))
	}

	if _, ok := values["style"]; ok {
		options.Conflicts = (h.getQuery("style") == "all_docs")
	}

	if _, ok := values["active_only"]; ok {
		options.ActiveOnly = h.getBoolQuery("active_only")
	}

	if _, ok := values["include_docs"]; ok {
		options.IncludeDocs = (h.getBoolQuery("include_docs"))
	}

	if _, ok := values["filter"]; ok {
		*filter = h.getQuery("filter")
	}

	if _, ok := values["channels"]; ok {
		channelsParam := h.getQuery("channels")
		if channelsParam != "" {
			channelsArray = strings.Split(channelsParam, ",")
		}
	}

	if _, ok := values["doc_ids"]; ok {
		docidsParam := h.getQuery("doc_ids")
		if docidsParam != "" {
			var querydocidKeys []string
			err := json.Unmarshal([]byte(docidsParam), &querydocidKeys)
			if err == nil {
				if len(querydocidKeys) > 0 {
					docIdsArray = querydocidKeys
				}
			} else {
				//This is not a JSON array so treat as a simple
				//comma separated list of doc id's
				docIdsArray = strings.Split(docidsParam, ",")
			}
		}
	}

	if _, ok := values["heartbeat"]; ok {
		options.HeartbeatMs = getRestrictedIntQuery(
			h.rq.URL.Query(),
			"heartbeat",
			kDefaultHeartbeatMS,
			kMinHeartbeatMS,
			h.server.config.MaxHeartbeat*1000,
			true,
		)
	}

	if _, ok := values["timeout"]; ok {
		options.TimeoutMs = getRestrictedIntQuery(
			h.rq.URL.Query(),
			"timeout",
			kDefaultTimeoutMS,
			0,
			kMaxTimeoutMS,
			true,
		)
	}
	return channelsArray, docIdsArray, nil
}

// Top-level handler for _changes feed requests. Accepts GET or POST requests.
func (h *handler) handleChanges() error {
	// http://wiki.apache.org/couchdb/HTTP_database_API#Changes
	// http://docs.couchdb.org/en/latest/api/database/changes.html
	base.StatsExpvars.Add("changesFeeds_total", 1)
	base.StatsExpvars.Add("changesFeeds_active", 1)
	defer base.StatsExpvars.Add("changesFeeds_active", -1)

	var feed string
	var options db.ChangesOptions
	var filter string
	var channelsArray []string
	var docIdsArray []string

	if h.rq.Method == "GET" {
		// GET request has parameters in URL:
		feed = h.getQuery("feed")
		var err error
		if options.Since, err = h.db.ParseSequenceID(h.getJSONStringQuery("since")); err != nil {
			return err
		}
		options.Limit = int(h.getIntQuery("limit", 0))
		options.Conflicts = (h.getQuery("style") == "all_docs")
		options.ActiveOnly = h.getBoolQuery("active_only")
		options.IncludeDocs = (h.getBoolQuery("include_docs"))
		filter = h.getQuery("filter")
		channelsParam := h.getQuery("channels")
		if channelsParam != "" {
			channelsArray = strings.Split(channelsParam, ",")
		}

		docidsParam := h.getQuery("doc_ids")
		if docidsParam != "" {
			var docidKeys []string
			err := json.Unmarshal([]byte(docidsParam), &docidKeys)
			if err == nil {
				if len(docidKeys) > 0 {
					docIdsArray = docidKeys
				}
			} else {
				//This is not a JSON array so treat as a simple
				//comma separated list of doc id's
				docIdsArray = strings.Split(docidsParam, ",")
			}
		}

		options.HeartbeatMs = getRestrictedIntQuery(
			h.rq.URL.Query(),
			"heartbeat",
			kDefaultHeartbeatMS,
			kMinHeartbeatMS,
			h.server.config.MaxHeartbeat*1000,
			true,
		)
		options.TimeoutMs = getRestrictedIntQuery(
			h.rq.URL.Query(),
			"timeout",
			kDefaultTimeoutMS,
			0,
			kMaxTimeoutMS,
			true,
		)

	} else {
		// POST request has parameters in JSON body:
		body, err := h.readBody()
		if err != nil {
			return err
		}
		feed, options, filter, channelsArray, docIdsArray, _, err = h.readChangesOptionsFromJSON(body)
		if err != nil {
			return err
		}
		channelsArray, docIdsArray, err = h.updateChangesOptionsFromQuery(&feed, &options, &filter, channelsArray, docIdsArray)
		if err != nil {
			return err
		}

	}

	// Get the channels as parameters to an imaginary "bychannel" filter.
	// The default is all channels the user can access.
	userChannels := ch.SetOf(ch.AllChannelWildcard)
	if filter != "" {
		if filter == "sync_gateway/bychannel" {
			if channelsArray == nil {
				return base.HTTPErrorf(http.StatusBadRequest, "Missing 'channels' filter parameter")
			}
			var err error
			userChannels, err = ch.SetFromArray(channelsArray, ch.ExpandStar)
			if err != nil {
				return err
			}
			if len(userChannels) == 0 {
				return base.HTTPErrorf(http.StatusBadRequest, "Empty channel list")
			}
		} else if filter == "_doc_ids" {
			if feed != "normal" && feed != "" {
				return base.HTTPErrorf(http.StatusBadRequest, "Filter '_doc_ids' is only valid for feed=normal replications")
			}
			if docIdsArray == nil {
				return base.HTTPErrorf(http.StatusBadRequest, "Missing 'doc_ids' filter parameter")
			}
			if len(docIdsArray) == 0 {
				return base.HTTPErrorf(http.StatusBadRequest, "Empty doc_ids list")
			}
		} else {
			return base.HTTPErrorf(http.StatusBadRequest, "Unknown filter; try sync_gateway/bychannel or _doc_ids")
		}
	}

	h.db.ChangesClientStats.Increment()
	defer h.db.ChangesClientStats.Decrement()

	options.Terminator = make(chan bool)

	var err error
	forceClose := false

	switch feed {
	case "normal", "":
		if filter == "_doc_ids" {
			err, forceClose = h.sendChangesForDocIds(userChannels, docIdsArray, options)
		} else {
			err, forceClose = h.sendSimpleChanges(userChannels, options)
		}
	case "longpoll":
		options.Wait = true
		err, forceClose = h.sendSimpleChanges(userChannels, options)
	case "continuous":
		err, forceClose = h.sendContinuousChangesByHTTP(userChannels, options)
	case "websocket":
		err, forceClose = h.sendContinuousChangesByWebSocket(userChannels, options)
	default:
		err = base.HTTPErrorf(http.StatusBadRequest, "Unknown feed type")
		forceClose = false
	}

	close(options.Terminator)

	if forceClose && h.user != nil {
		h.db.DatabaseContext.NotifyUser(h.user.Name())
	}

	return err
}

func (h *handler) sendSimpleChanges(channels base.Set, options db.ChangesOptions) (error, bool) {
	lastSeq := options.Since
	var first bool = true
	feed, err := h.db.MultiChangesFeed(channels, options)
	if err != nil {
		return err, false
	}

	h.setHeader("Content-Type", "application/json")
	h.setHeader("Cache-Control", "private, max-age=0, no-cache, no-store")
	h.response.Write([]byte("{\"results\":[\r\n"))
	if options.Wait {
		h.flush()
	}
	message := "OK"
	forceClose := false
	if feed != nil {
		var heartbeat, timeout <-chan time.Time
		if options.Wait {
			// Set up heartbeat/timeout
			if options.HeartbeatMs > 0 {
				ticker := time.NewTicker(time.Duration(options.HeartbeatMs) * time.Millisecond)
				defer ticker.Stop()
				heartbeat = ticker.C
			} else if options.TimeoutMs > 0 {
				timer := time.NewTimer(time.Duration(options.TimeoutMs) * time.Millisecond)
				defer timer.Stop()
				timeout = timer.C
			}
		}

		var closeNotify <-chan bool
		cn, ok := h.response.(http.CloseNotifier)
		if ok {
			closeNotify = cn.CloseNotify()
		} else {
			base.LogTo("Changes", "simple changes cannot get Close Notifier from ResponseWriter")
		}

		encoder := json.NewEncoder(h.response)
	loop:
		for {
			select {
			case entry, ok := <-feed:
				if !ok {
					break loop // end of feed
				}
				if nil != entry {
					if entry.Err != nil {
						break loop // error returned by feed - end changes
					}
					if first {
						first = false
					} else {
						h.response.Write([]byte(","))
					}
					encoder.Encode(entry)
					lastSeq = entry.Seq
				}

			case <-heartbeat:
				_, err = h.response.Write([]byte("\n"))
				h.flush()
				base.LogTo("Heartbeat", "heartbeat written to _changes feed for request received %s", h.currentEffectiveUserName())
			case <-timeout:
				message = "OK (timeout)"
				forceClose = true
				break loop
			case <-closeNotify:
				base.LogTo("Changes", "Connection lost from client: %v", h.currentEffectiveUserName())
				forceClose = true
				break loop
			case <-h.db.ExitChanges:
				message = "OK DB has gone offline"
				forceClose = true
				break loop
			}
			if err != nil {
				h.logStatus(599, fmt.Sprintf("Write error: %v", err))
				return nil, forceClose // error is probably because the client closed the connection
			}
		}
	}

	s := fmt.Sprintf("],\n\"last_seq\":%q}\n", lastSeq.String())
	h.response.Write([]byte(s))
	h.logStatus(http.StatusOK, message)
	return nil, forceClose
}

/*
 * Generate the changes for a specific list of doc ID's, only documents accesible to the user will generate
 * results
 */
func (h *handler) sendChangesForDocIds(userChannels base.Set, explicitDocIds []string, options db.ChangesOptions) (error, bool) {

	// Subroutine that creates a response row for a document:
	first := true
	var lastSeq uint64 = 0
	//rowMap := make(map[uint64]*changesRow)
	rowMap := make(map[uint64]*db.ChangeEntry)

	createRow := func(doc db.IDAndRev) *db.ChangeEntry {
		row := &db.ChangeEntry{ID: doc.DocID}

		// Fetch the document body and other metadata that lives with it:
		populatedDoc, body, err := h.db.GetDocAndActiveRev(doc.DocID)
		if err != nil {
			base.LogTo("Changes", "Unable to get changes for docID %v, caused by %v", doc.DocID, err)
			return nil
		}

		if populatedDoc.Sequence <= options.Since.Seq {
			return nil
		}

		if body == nil {
			return nil
		}

		changes := make([]db.ChangeRev, 1)
		changes[0] = db.ChangeRev{"rev": body["_rev"].(string)}
		row.Changes = changes
		row.Seq = db.SequenceID{Seq: populatedDoc.Sequence}
		row.SetBranched((populatedDoc.Flags & ch.Branched) != 0)

		var removedChannels []string

		if deleted, _ := body["_deleted"].(bool); deleted {
			row.Deleted = true
		}

		userCanSeeDocChannel := false

		if h.user == nil || h.user.Channels().Contains(ch.UserStarChannel) {
			userCanSeeDocChannel = true
		} else if len(populatedDoc.Channels) > 0 {
			//Do special _removed/_deleted processing
			for channel, removal := range populatedDoc.Channels {
				//Doc is tagged with channel or was removed at a sequence later that since sequence
				if removal == nil || removal.Seq > options.Since.Seq {
					//if the current user has access to this channel
					if h.user.CanSeeChannel(channel) {
						userCanSeeDocChannel = true
						//If the doc has been removed
						if removal != nil {
							removedChannels = append(removedChannels, channel)
							if removal.Deleted {
								row.Deleted = true
							}
						}
					}
				}
			}
		}

		if !userCanSeeDocChannel {
			return nil
		}

		row.Removed = base.SetFromArray(removedChannels)
		if options.IncludeDocs || options.Conflicts {
			h.db.AddDocInstanceToChangeEntry(row, populatedDoc, options)
		}

		return row
	}

	h.setHeader("Content-Type", "application/json")
	h.setHeader("Cache-Control", "private, max-age=0, no-cache, no-store")
	h.response.Write([]byte("{\"results\":[\r\n"))

	var keys base.Uint64Slice

	for _, docID := range explicitDocIds {
		row := createRow(db.IDAndRev{DocID: docID, RevID: "", Sequence: 0})
		if row != nil {
			rowMap[row.Seq.Seq] = row
			keys = append(keys, row.Seq.Seq)
		}
	}

	//Write out rows sorted by sequenceID
	keys.Sort()
	for _, k := range keys {
		if first {
			first = false
		} else {
			h.response.Write([]byte(","))
		}
		h.addJSON(rowMap[k])

		lastSeq = k

		if options.Limit > 0 {
			options.Limit--
			if options.Limit == 0 {
				break
			}
		}
	}

	s := fmt.Sprintf("],\n\"last_seq\":%d}\n", lastSeq)
	h.response.Write([]byte(s))
	h.logStatus(http.StatusOK, "OK")
	return nil, false
}

// This is the core functionality of both the HTTP and WebSocket-based continuous change feed.
// It defers to a callback function 'send()' to actually send the changes to the client.
// It will call send(nil) to notify that it's caught up and waiting for new changes, or as
// a periodic heartbeat while waiting.
func (h *handler) generateContinuousChanges(inChannels base.Set, options db.ChangesOptions, send func([]*db.ChangeEntry) error) (error, bool) {
	// Set up heartbeat/timeout
	var timeoutInterval time.Duration
	var timer *time.Timer
	var heartbeat <-chan time.Time
	if options.HeartbeatMs > 0 {
		ticker := time.NewTicker(time.Duration(options.HeartbeatMs) * time.Millisecond)
		defer ticker.Stop()
		heartbeat = ticker.C
	} else if options.TimeoutMs > 0 {
		timeoutInterval = time.Duration(options.TimeoutMs) * time.Millisecond
		defer func() {
			if timer != nil {
				timer.Stop()
			}
		}()
	}

	options.Wait = true       // we want the feed channel to wait for changes
	options.Continuous = true // and to keep sending changes indefinitely
	var lastSeq db.SequenceID
	var feed <-chan *db.ChangeEntry
	var timeout <-chan time.Time
	var err error

	var closeNotify <-chan bool
	cn, ok := h.response.(http.CloseNotifier)
	if ok {
		closeNotify = cn.CloseNotify()
	} else {
		base.LogTo("Changes", "continuous changes cannot get Close Notifier from ResponseWriter")
	}

	forceClose := false

loop:
	for {
		if feed == nil {
			// Refresh the feed of all current changes:
			if lastSeq.IsNonZero() { // start after end of last feed
				options.Since = lastSeq
			}
			if h.db.IsClosed() {
				forceClose = true
				break loop
			}
			feed, err = h.db.MultiChangesFeed(inChannels, options)
			if err != nil || feed == nil {
				return err, forceClose
			}
		}

		if timeoutInterval > 0 && timer == nil {
			// Timeout resets after every change is sent
			timer = time.NewTimer(timeoutInterval)
			timeout = timer.C
		}

		// Wait for either a new change, a heartbeat, or a timeout:
		select {
		case entry, ok := <-feed:
			if !ok {
				feed = nil
			} else if entry == nil {
				err = send(nil)
			} else if entry.Err != nil {
				break loop // error returned by feed - end changes
			} else {
				entries := []*db.ChangeEntry{entry}
				waiting := false
				// Batch up as many entries as we can without waiting:
			collect:
				for len(entries) < 20 {
					select {
					case entry, ok = <-feed:
						if !ok {
							feed = nil
							break collect
						} else if entry == nil {
							waiting = true
							break collect
						} else if entry.Err != nil {
							break loop // error returned by feed - end changes
						}
						entries = append(entries, entry)
					default:
						break collect
					}
				}
				base.LogTo("Changes", "sending %d change(s)", len(entries))
				err = send(entries)

				if err == nil && waiting {
					err = send(nil)
				}

				lastSeq = entries[len(entries)-1].Seq
				if options.Limit > 0 {
					if len(entries) >= options.Limit {
						forceClose = true
						break loop
					}
					options.Limit -= len(entries)
				}
			}
			// Reset the timeout after sending an entry:
			if timer != nil {
				timer.Stop()
				timer = nil
			}
		case <-heartbeat:
			err = send(nil)
			base.LogTo("Heartbeat", "heartbeat written to _changes feed for request received %s", h.currentEffectiveUserName())
		case <-timeout:
			forceClose = true
			break loop
		case <-closeNotify:
			base.LogTo("Changes", "Connection lost from client: %v", h.currentEffectiveUserName())
			forceClose = true
			break loop
		case <-h.db.ExitChanges:
			forceClose = true
			break loop
		}

		if err != nil {
			h.logStatus(http.StatusOK, fmt.Sprintf("Write error: %v", err))
			return nil, forceClose // error is probably because the client closed the connection
		}
	}

	h.logStatus(http.StatusOK, "OK (continuous feed closed)")
	return nil, forceClose
}

func (h *handler) sendContinuousChangesByHTTP(inChannels base.Set, options db.ChangesOptions) (error, bool) {
	// Setting a non-default content type will keep the client HTTP framework from trying to sniff
	// a real content-type from the response text, which can delay or prevent the client app from
	// receiving the response.
	h.setHeader("Content-Type", "application/octet-stream")
	h.setHeader("Cache-Control", "private, max-age=0, no-cache, no-store")
	h.logStatus(http.StatusOK, "sending continuous feed")
	return h.generateContinuousChanges(inChannels, options, func(changes []*db.ChangeEntry) error {
		var err error
		if changes != nil {
			for _, change := range changes {
				data, _ := json.Marshal(change)
				if _, err = h.response.Write(data); err != nil {
					break
				}
				if _, err = h.response.Write([]byte("\n")); err != nil {
					break
				}
			}
		} else {
			_, err = h.response.Write([]byte("\n"))
		}
		h.flush()
		return err
	})
}

func (h *handler) sendContinuousChangesByWebSocket(inChannels base.Set, options db.ChangesOptions) (error, bool) {

	forceClose := false
	handler := func(conn *websocket.Conn) {
		h.logStatus(101, "Upgraded to WebSocket protocol")
		defer func() {
			conn.Close()
			base.LogTo("HTTP+", "#%03d:     --> WebSocket closed", h.serialNumber)
		}()

		// Read changes-feed options from an initial incoming WebSocket message in JSON format:
		var wsoptions db.ChangesOptions
		var compress bool
		if msg, err := readWebSocketMessage(conn); err != nil {
			return
		} else {
			var channelNames []string
			var err error
			if _, wsoptions, _, channelNames, _, compress, err = h.readChangesOptionsFromJSON(msg); err != nil {
				return
			}
			if channelNames != nil {
				inChannels, _ = ch.SetFromArray(channelNames, ch.ExpandStar)
			}
		}

		//Copy options.Terminator to new WebSocket options
		//options.Terminator will be closed automatically when
		//changes feed completes
		wsoptions.Terminator = options.Terminator

		// Set up GZip compression
		var writer *bytes.Buffer
		var zipWriter *gzip.Writer
		if compress {
			writer = bytes.NewBuffer(nil)
			zipWriter = GetGZipWriter(writer)
		}

		caughtUp := false
		_, forceClose = h.generateContinuousChanges(inChannels, wsoptions, func(changes []*db.ChangeEntry) error {
			var data []byte
			if changes != nil {
				data, _ = json.Marshal(changes)
			} else if !caughtUp {
				caughtUp = true
				data, _ = json.Marshal([]*db.ChangeEntry{})
			} else {
				data = []byte{}
			}
			if compress && len(data) > 8 {
				// Compress JSON, using same GZip context, and send as binary msg:
				zipWriter.Write(data)
				zipWriter.Flush()
				data = writer.Bytes()
				writer.Reset()
				conn.PayloadType = websocket.BinaryFrame
			} else {
				conn.PayloadType = websocket.TextFrame
			}
			_, err := conn.Write(data)
			return err
		})

		if zipWriter != nil {
			ReturnGZipWriter(zipWriter)
		}
	}
	server := websocket.Server{
		Handshake: func(*websocket.Config, *http.Request) error { return nil },
		Handler:   handler,
	}
	server.ServeHTTP(h.response, h.rq)
	return nil, forceClose
}

func (h *handler) readChangesOptionsFromJSON(jsonData []byte) (feed string, options db.ChangesOptions, filter string, channelsArray []string, docIdsArray []string, compress bool, err error) {
	var input struct {
		Feed           string        `json:"feed"`
		Since          db.SequenceID `json:"since"`
		Limit          int           `json:"limit"`
		Style          string        `json:"style"`
		IncludeDocs    bool          `json:"include_docs"`
		Filter         string        `json:"filter"`
		Channels       string        `json:"channels"` // a filter query param, so it has to be a string
		DocIds         []string      `json:"doc_ids"`
		HeartbeatMs    *uint64       `json:"heartbeat"`
		TimeoutMs      *uint64       `json:"timeout"`
		AcceptEncoding string        `json:"accept_encoding"`
		ActiveOnly     bool          `json:"active_only"` // Return active revisions only
	}
	// Initialize since clock and hasher ahead of unmarshalling sequence
	if h.db != nil && h.db.SequenceType == db.ClockSequenceType {
		input.Since.Clock = base.NewSequenceClockImpl()
		input.Since.SeqType = h.db.SequenceType
		input.Since.SequenceHasher = h.db.SequenceHasher
	}
	if err = json.Unmarshal(jsonData, &input); err != nil {
		return
	}
	feed = input.Feed
	options.Since = input.Since
	options.Limit = input.Limit

	options.Conflicts = input.Style == "all_docs"
	options.ActiveOnly = input.ActiveOnly

	options.IncludeDocs = input.IncludeDocs
	filter = input.Filter

	if input.Channels != "" {
		channelsArray = strings.Split(input.Channels, ",")
	}

	docIdsArray = input.DocIds

	options.HeartbeatMs = getRestrictedInt(
		input.HeartbeatMs,
		kDefaultHeartbeatMS,
		kMinHeartbeatMS,
		h.server.config.MaxHeartbeat*1000,
		true,
	)

	options.TimeoutMs = getRestrictedInt(
		input.TimeoutMs,
		kDefaultTimeoutMS,
		0,
		kMaxTimeoutMS,
		true,
	)

	compress = (input.AcceptEncoding == "gzip")

	return
}

// Helper function to read a complete message from a WebSocket (because the API makes it hard)
func readWebSocketMessage(conn *websocket.Conn) ([]byte, error) {
	var message []byte
	buf := make([]byte, 100)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return nil, err
		}
		message = append(message, buf[0:n]...)
		if n < len(buf) {
			break
		}
	}
	return message, nil
}

func sequenceFromString(str string) uint64 {
	seq, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		seq = 0
	}
	return seq
}
