//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"golang.org/x/net/websocket"
)

// Minimum value of _changes?heartbeat property
const kMinHeartbeatMS = 25 * 1000

// Default value of _changes?heartbeat property
const kDefaultHeartbeatMS = 0

// Default value of _changes?timeout property
const kDefaultTimeoutMS = 5 * 60 * 1000

// Maximum value of _changes?timeout property
const kMaxTimeoutMS = 15 * 60 * 1000

// Values for feed parameter on changes request
const feedTypeContinuous = "continuous"
const feedTypeLongpoll = "longpoll"
const feedTypeNormal = "normal"
const feedTypeWebsocket = "websocket"

func (h *handler) handleRevsDiff() error {
	var input map[string][]string
	err := h.readJSONInto(&input)
	if err != nil {
		return err
	}

	_, _ = h.response.Write([]byte("{"))
	first := true
	for docid, revs := range input {
		missing, possible := h.collection.RevDiff(h.ctx(), docid, revs)
		if missing != nil {
			docOutput := map[string]interface{}{"missing": missing}
			if possible != nil {
				docOutput["possible_ancestors"] = possible
			}
			if !first {
				_, _ = h.response.Write([]byte(",\n"))
			}
			first = false
			_, _ = h.response.Write([]byte(fmt.Sprintf("%q:", docid)))
			err = h.addJSON(docOutput)
			if err != nil {
				return err
			}
		}
	}
	_, _ = h.response.Write([]byte("}"))
	return nil
}

// UpdateChangesOptionsFromQuery handles any changes POST requests that send parameters in the POST body AND in the query string.  If any parameters
// are present in the query string, they override the values sent in the body.

func (h *handler) updateChangesOptionsFromQuery(feed *string, options *db.ChangesOptions, filter *string, channelsArray []string, docIdsArray []string) (newChannelsArray []string, newDocIdsArray []string, err error) {

	if h.rq.URL.RawQuery == "" {
		return channelsArray, docIdsArray, nil
	}

	values := h.getQueryValues()

	if _, ok := values["feed"]; ok {
		*feed = h.getQuery("feed")
	}

	if _, ok := values["since"]; ok {
		if options.Since, err = db.ParsePlainSequenceID(h.getJSONStringQuery("since")); err != nil {
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
			err := base.JSONUnmarshal([]byte(docidsParam), &querydocidKeys)
			if err == nil {
				if len(querydocidKeys) > 0 {
					docIdsArray = querydocidKeys
				}
			} else {
				// This is not a JSON array so treat as a simple
				// comma separated list of doc id's
				docIdsArray = strings.Split(docidsParam, ",")
			}
		}
	}

	if _, ok := values["heartbeat"]; ok {
		options.HeartbeatMs = base.GetRestrictedIntQuery(
			h.getQueryValues(),
			"heartbeat",
			kDefaultHeartbeatMS,
			kMinHeartbeatMS,
			uint64(h.server.Config.Replicator.MaxHeartbeat.Value().Milliseconds()),
			true,
		)
	}

	if _, ok := values["timeout"]; ok {
		options.TimeoutMs = base.GetRestrictedIntQuery(
			h.getQueryValues(),
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

	var feed string
	var options db.ChangesOptions
	var filter string
	var channelsArray []string
	var docIdsArray []string

	if h.rq.Method == "GET" {
		// GET request has parameters in URL:
		feed = h.getQuery("feed")
		var err error
		if options.Since, err = db.ParsePlainSequenceID(h.getJSONStringQuery("since")); err != nil {
			return err
		}
		options.Limit = int(h.getIntQuery("limit", 0))
		options.Conflicts = h.getQuery("style") == "all_docs"
		options.ActiveOnly = h.getBoolQuery("active_only")
		options.IncludeDocs = h.getBoolQuery("include_docs")
		options.Revocations = h.getBoolQuery("revocations")

		useRequestPlus, _ := h.getOptBoolQuery("request_plus", h.db.Options.ChangesRequestPlus)
		if useRequestPlus && feed != feedTypeContinuous {
			var seqErr error
			options.RequestPlusSeq, seqErr = h.db.GetRequestPlusSequence()
			if seqErr != nil {
				return base.HTTPErrorf(http.StatusServiceUnavailable, "Unable to retrieve requestPlus sequence")
			}

		}
		filter = h.getQuery("filter")
		channelsParam := h.getQuery("channels")
		if channelsParam != "" {
			channelsArray = strings.Split(channelsParam, ",")
		}

		docidsParam := h.getQuery("doc_ids")
		if docidsParam != "" {
			var docidKeys []string
			err := base.JSONUnmarshal([]byte(docidsParam), &docidKeys)
			if err == nil {
				if len(docidKeys) > 0 {
					docIdsArray = docidKeys
				}
			} else {
				// This is not a JSON array so treat as a simple
				// comma separated list of doc id's
				docIdsArray = strings.Split(docidsParam, ",")
			}
		}
		options.HeartbeatMs = base.GetRestrictedIntQuery(
			h.getQueryValues(),
			"heartbeat",
			kDefaultHeartbeatMS,
			kMinHeartbeatMS,
			uint64(h.server.Config.Replicator.MaxHeartbeat.Value().Milliseconds()),
			true,
		)
		options.TimeoutMs = base.GetRestrictedIntQuery(
			h.getQueryValues(),
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

		to := ""
		if h.user != nil && h.user.Name() != "" {
			to = fmt.Sprintf("  (to %s)", h.user.Name())
		}

		base.DebugfCtx(h.ctx(), base.KeyChanges, "Changes POST request.  URL: %v, feed: %v, options: %+v, filter: %v, bychannel: %v, docIds: %v %s",
			h.rq.URL, feed, options, filter, base.UD(channelsArray), base.UD(docIdsArray), base.UD(to))

	}

	// Default to feed type normal
	if feed == "" {
		feed = "normal"
	}

	needRelease, concurrentReplicationsErr := h.server.incrementConcurrentReplications(h.rqCtx)
	if concurrentReplicationsErr != nil {
		return concurrentReplicationsErr
	}
	// if we haven't incremented the active replicator due to MaxConcurrentReplications being 0, we don't need to decrement it
	if needRelease {
		defer h.server.decrementConcurrentReplications(h.rqCtx)
	}

	// Get the channels as parameters to an imaginary "bychannel" filter.
	// The default is all channels the user can access.
	userChannels := base.SetOf(ch.AllChannelWildcard)
	if filter != "" {
		if filter == base.ByChannelFilter {
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
			if feed != "normal" {
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

	// Pull replication stats by type
	if feed == "normal" {
		h.db.DatabaseContext.DbStats.CBLReplicationPull().NumPullReplActiveOneShot.Add(1)
		h.db.DatabaseContext.DbStats.CBLReplicationPull().NumPullReplTotalOneShot.Add(1)
		defer h.db.DatabaseContext.DbStats.CBLReplicationPull().NumPullReplActiveOneShot.Add(-1)
	} else {
		h.db.DbStats.CBLReplicationPull().NumPullReplActiveContinuous.Add(1)
		h.db.DbStats.CBLReplicationPull().NumPullReplTotalContinuous.Add(1)
		defer h.db.DbStats.CBLReplicationPull().NumPullReplActiveContinuous.Add(-1)
	}

	// Overall replication counts
	h.db.DatabaseContext.DbStats.Database().NumReplicationsActive.Add(1)
	h.db.DatabaseContext.DbStats.Database().NumReplicationsTotal.Add(1)
	defer h.db.DatabaseContext.DbStats.Database().NumReplicationsActive.Add(-1)

	changesCtx, changesCtxCancel := context.WithCancel(context.Background())
	options.ChangesCtx = changesCtx

	forceClose := false

	var err error

	switch feed {
	case feedTypeNormal:
		if filter == "_doc_ids" {
			err, forceClose = h.sendSimpleChanges(userChannels, options, docIdsArray)
		} else {
			err, forceClose = h.sendSimpleChanges(userChannels, options, nil)
		}
	case feedTypeLongpoll:
		options.Wait = true
		err, forceClose = h.sendSimpleChanges(userChannels, options, nil)
	case feedTypeContinuous:
		err, forceClose = h.sendContinuousChangesByHTTP(userChannels, options)
	case feedTypeWebsocket:
		err, forceClose = h.sendContinuousChangesByWebSocket(userChannels, options)
	default:
		err = base.HTTPErrorf(http.StatusBadRequest, "Unknown feed type")
		forceClose = false
	}

	changesCtxCancel()

	// On forceClose, send notify to trigger immediate exit from change waiter
	if forceClose {
		user := ""
		if h.user != nil {
			user = h.user.Name()
		}
		h.db.DatabaseContext.NotifyTerminatedChanges(user)
	}

	return err
}

func (h *handler) sendSimpleChanges(channels base.Set, options db.ChangesOptions, docids []string) (error, bool) {
	lastSeq := options.Since
	var first bool = true
	var feed <-chan *db.ChangeEntry
	var err error
	if len(docids) > 0 {
		feed, err = h.collection.DocIDChangesFeed(h.ctx(), channels, docids, options)
	} else {
		feed, err = h.collection.MultiChangesFeed(h.ctx(), channels, options)
	}
	if err != nil {
		return err, false
	}

	h.setHeader("Content-Type", "application/json")
	h.setHeader("Cache-Control", "private, max-age=0, no-cache, no-store")
	_, _ = h.response.Write([]byte("{\"results\":[\r\n"))

	logStatus := h.logStatusWithDuration

	if options.Wait {
		logStatus = h.logStatus
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

		encoder := base.JSONEncoderCanonical(h.response)
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
						_, _ = h.response.Write([]byte(","))
					}
					_ = encoder.Encode(entry)
					lastSeq = entry.Seq
				}

			case <-heartbeat:
				_, err = h.response.Write([]byte("\n"))
				h.flush()
				base.DebugfCtx(h.ctx(), base.KeyChanges, "heartbeat written to _changes feed for request received")
			case <-timeout:
				message = "OK (timeout)"
				forceClose = true
				break loop
			case <-h.rq.Context().Done():
				base.InfofCtx(h.ctx(), base.KeyChanges, "Connection lost from client")
				forceClose = true
				break loop
			case <-h.db.ExitChanges:
				message = "OK DB has gone offline"
				forceClose = true
				break loop
			}
			if err != nil {
				logStatus(599, fmt.Sprintf("Write error: %v", err))
				return nil, forceClose // error is probably because the client closed the connection
			}
		}
	}

	s := fmt.Sprintf("],\n\"last_seq\":%q}\n", lastSeq.String())
	_, _ = h.response.Write([]byte(s))
	logStatus(http.StatusOK, message)
	return nil, forceClose
}

// This is the core functionality of both the HTTP and WebSocket-based continuous change feed.
// It defers to a callback function 'send()' to actually send the changes to the client.
// It will call send(nil) to notify that it's caught up and waiting for new changes, or as
// a periodic heartbeat while waiting.
func (h *handler) generateContinuousChanges(inChannels base.Set, options db.ChangesOptions, send func([]*db.ChangeEntry) error) (error, bool) {
	// Ensure continuous is set, since generateChanges now supports both continuous and one-shot
	options.Continuous = true
	err, forceClose := db.GenerateChanges(h.ctx(), h.rq.Context(), h.collection, inChannels, options, nil, send)
	if sendErr, ok := err.(*db.ChangesSendErr); ok {
		h.logStatus(http.StatusOK, fmt.Sprintf("Write error: %v", sendErr))
		return nil, forceClose // error is probably because the client closed the connection
	} else {
		h.logStatus(http.StatusOK, "OK (continuous feed closed)")
	}
	return err, forceClose
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
				data, _ := base.JSONMarshal(change)
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
			if err := conn.Close(); err != nil {
				base.WarnfCtx(h.ctx(), "WebSocket connection (%s) closed with error %v", h.formatSerialNumber(), err)
			}
			base.InfofCtx(h.ctx(), base.KeyHTTP, "%s:     --> WebSocket closed", h.formatSerialNumber())
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

		// Copy options.ChangesCtx to new WebSocket options
		// options.ChangesCtx will be cancelled automatically when
		// changes feed completes
		wsoptions.ChangesCtx = options.ChangesCtx

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
				data, _ = base.JSONMarshal(changes)
			} else if !caughtUp {
				caughtUp = true
				data, _ = base.JSONMarshal([]*db.ChangeEntry{})
			} else {
				data = []byte{}
			}
			if compress && len(data) > 8 {
				// Compress JSON, using same GZip context, and send as binary msg:
				_, _ = zipWriter.Write(data)
				_ = zipWriter.Flush()
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
		ActiveOnly     bool          `json:"active_only"`  // Return active revisions only
		RequestPlus    *bool         `json:"request_plus"` // Wait for sequence buffering to catch up to database seq value at time request was issued
	}

	// Initialize since clock and hasher ahead of unmarshalling sequence
	if h.db != nil {
		input.Since = db.CreateZeroSinceValue()
	}

	if err = base.JSONUnmarshal(jsonData, &input); err != nil {
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
	options.HeartbeatMs = base.GetRestrictedInt(
		input.HeartbeatMs,
		kDefaultHeartbeatMS,
		kMinHeartbeatMS,
		uint64(h.server.Config.Replicator.MaxHeartbeat.Value().Milliseconds()),
		true,
	)

	options.TimeoutMs = base.GetRestrictedInt(
		input.TimeoutMs,
		kDefaultTimeoutMS,
		0,
		kMaxTimeoutMS,
		true,
	)

	compress = (input.AcceptEncoding == "gzip")

	if h.db != nil && feed != feedTypeContinuous {
		useRequestPlus := h.db.Options.ChangesRequestPlus
		if input.RequestPlus != nil {
			useRequestPlus = *input.RequestPlus
		}
		if useRequestPlus {
			var seqErr error
			options.RequestPlusSeq, seqErr = h.db.GetRequestPlusSequence()
			if seqErr != nil {
				err = base.HTTPErrorf(http.StatusServiceUnavailable, "Unable to retrieve requestPlus sequence: %v", seqErr)
				return
			}
		}
	}
	return
}

// Helper function to read a complete message from a WebSocket
func readWebSocketMessage(conn *websocket.Conn) ([]byte, error) {

	var message []byte
	if err := websocket.Message.Receive(conn, &message); err != nil {
		if err != io.EOF {
			base.WarnfCtx(context.TODO(), "Error reading initial websocket message: %v", err)
			return nil, err
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
