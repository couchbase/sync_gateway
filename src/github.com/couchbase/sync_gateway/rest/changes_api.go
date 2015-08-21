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
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/websocket"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
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

// Top-level handler for _changes feed requests. Accepts GET or POST requests.
func (h *handler) handleChanges() error {
	// http://wiki.apache.org/couchdb/HTTP_database_API#Changes
	// http://docs.couchdb.org/en/latest/api/database/changes.html
	restExpvars.Add("changesFeeds_total", 1)
	restExpvars.Add("changesFeeds_active", 1)
	defer restExpvars.Add("changesFeeds_active", -1)

	var feed string
	var options db.ChangesOptions
	var filter string
	var channelsArray []string
	if h.rq.Method == "GET" {
		// GET request has parameters in URL:
		feed = h.getQuery("feed")
		var err error
		if err = options.Since.UnmarshalJSON([]byte(h.getQuery("since"))); err != nil {
			return err
		}
		options.Limit = int(h.getIntQuery("limit", 0))
		options.Conflicts = (h.getQuery("style") == "all_docs")
		options.IncludeDocs = (h.getBoolQuery("include_docs"))
		filter = h.getQuery("filter")
		channelsParam := h.getQuery("channels")
		if channelsParam != "" {
			channelsArray = strings.Split(channelsParam, ",")
		}
		options.HeartbeatMs = getRestrictedIntQuery(h.rq.URL.Query(), "heartbeat", kDefaultHeartbeatMS, kMinHeartbeatMS, h.server.config.MaxHeartbeat*1000, true)
		options.TimeoutMs = getRestrictedIntQuery(h.rq.URL.Query(), "timeout", kDefaultTimeoutMS, 0, kMaxTimeoutMS, true)

	} else {
		// POST request has parameters in JSON body:
		body, err := h.readBody()
		if err != nil {
			return err
		}
		feed, options, filter, channelsArray, err = h.readChangesOptionsFromJSON(body)
		if err != nil {
			return err
		}
	}

	// Get the channels as parameters to an imaginary "bychannel" filter.
	// The default is all channels the user can access.
	userChannels := channels.SetOf(channels.AllChannelWildcard)
	if filter != "" {
		if filter != "sync_gateway/bychannel" {
			return base.HTTPErrorf(http.StatusBadRequest, "Unknown filter; try sync_gateway/bychannel")
		}
		if channelsArray == nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Missing 'channels' filter parameter")
		}
		var err error
		userChannels, err = channels.SetFromArray(channelsArray, channels.ExpandStar)
		if err != nil {
			return err
		}
		if len(userChannels) == 0 {
			return base.HTTPErrorf(http.StatusBadRequest, "Empty channel list")
		}
	}

	h.db.ChangesClientStats.Increment()
	defer h.db.ChangesClientStats.Decrement()

	options.Terminator = make(chan bool)
	defer close(options.Terminator)

	switch feed {
	case "normal", "":
		return h.sendSimpleChanges(userChannels, options)
	case "longpoll":
		options.Wait = true
		return h.sendSimpleChanges(userChannels, options)
	case "continuous":
		return h.sendContinuousChangesByHTTP(userChannels, options)
	case "websocket":
		return h.sendContinuousChangesByWebSocket(userChannels, options)
	default:
		return base.HTTPErrorf(http.StatusBadRequest, "Unknown feed type")
	}
}

func (h *handler) sendSimpleChanges(channels base.Set, options db.ChangesOptions) error {
	lastSeq := options.Since
	var first bool = true
	feed, err := h.db.MultiChangesFeed(channels, options)
	if err != nil {
		return err
	}

	h.setHeader("Content-Type", "application/json")
	h.setHeader("Cache-Control", "private, max-age=0, no-cache, no-store")
	h.response.Write([]byte("{\"results\":[\r\n"))
	if options.Wait {
		h.flush()
	}
	message := "OK"
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
				break loop
			}
			if err != nil {
				h.logStatus(599, fmt.Sprintf("Write error: %v", err))
				return nil // error is probably because the client closed the connection
			}
		}
	}
	s := fmt.Sprintf("],\n\"last_seq\":%q}\n", lastSeq.String())
	h.response.Write([]byte(s))
	h.logStatus(http.StatusOK, message)
	return nil
}

// This is the core functionality of both the HTTP and WebSocket-based continuous change feed.
// It defers to a callback function 'send()' to actually send the changes to the client.
// It will call send(nil) to notify that it's caught up and waiting for new changes, or as
// a periodic heartbeat while waiting.
func (h *handler) generateContinuousChanges(inChannels base.Set, options db.ChangesOptions, send func([]*db.ChangeEntry) error) error {
	err := generateContinuousChanges(h.db, inChannels, options, nil, send)
	h.logStatus(http.StatusOK, "OK (continuous feed closed)")
	return err
}

// Shell of the continuous changes feed -- calls out to a `send` function to deliver the change.
// This is called from BLIP connections as well as HTTP handlers, which is why this is not a
// method on `handler`. (In the BLIP case the `h` parameter will be nil.)
func generateContinuousChanges(database *db.Database, inChannels base.Set, options db.ChangesOptions, h *handler, send func([]*db.ChangeEntry) error) error {
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

loop:
	for {
		if feed == nil {
			// Refresh the feed of all current changes:
			if lastSeq.Seq > 0 { // start after end of last feed
				options.Since = lastSeq
			}
			if database.IsClosed() {
				break loop
			}
			feed, err = database.MultiChangesFeed(inChannels, options)
			if err != nil || feed == nil {
				return err
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
			if h != nil {
				base.LogTo("Heartbeat", "heartbeat written to _changes feed for request received %s", h.currentEffectiveUserName())
			}
		case <-timeout:
			break loop
		}

		if err != nil {
			//h.logStatus(http.StatusOK, fmt.Sprintf("Write error: %v", err))
			return nil // error is probably because the client closed the connection
		}
	}
	return nil
}

func (h *handler) sendContinuousChangesByHTTP(inChannels base.Set, options db.ChangesOptions) error {
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

func (h *handler) sendContinuousChangesByWebSocket(inChannels base.Set, options db.ChangesOptions) error {
	handler := func(conn *websocket.Conn) {
		h.logStatus(101, "Upgraded to WebSocket protocol")
		defer func() {
			conn.Close()
			base.LogTo("HTTP+", "#%03d:     --> WebSocket closed", h.serialNumber)
		}()

		// Read changes-feed options from an initial incoming WebSocket message in JSON format:
		if msg, err := readWebSocketMessage(conn); err != nil {
			return
		} else {
			var channelNames []string
			var err error
			if _, options, _, channelNames, err = h.readChangesOptionsFromJSON(msg); err != nil {
				return
			}
			if channelNames != nil {
				inChannels, _ = channels.SetFromArray(channelNames, channels.ExpandStar)
			}
		}

		options.Terminator = make(chan bool)
		defer close(options.Terminator)

		caughtUp := false
		h.generateContinuousChanges(inChannels, options, func(changes []*db.ChangeEntry) error {
			var data []byte
			if changes != nil {
				data, _ = json.Marshal(changes)
			} else if !caughtUp {
				caughtUp = true
				data, _ = json.Marshal([]*db.ChangeEntry{})
			} else {
				data = []byte{}
			}
			_, err := conn.Write(data)
			return err
		})
	}
	server := websocket.Server{
		Handshake: func(*websocket.Config, *http.Request) error { return nil },
		Handler:   handler,
	}
	server.ServeHTTP(h.response, h.rq)
	return nil
}

func (h *handler) readChangesOptionsFromJSON(jsonData []byte) (feed string, options db.ChangesOptions, filter string, channelsArray []string, err error) {
	var input struct {
		Feed        string        `json:"feed"`
		Since       db.SequenceID `json:"since"`
		Limit       int           `json:"limit"`
		Style       string        `json:"style"`
		IncludeDocs bool          `json:"include_docs"`
		Filter      string        `json:"filter"`
		Channels    string        `json:"channels"` // a filter query param, so it has to be a string
		HeartbeatMs *uint64       `json:"heartbeat"`
		TimeoutMs   *uint64       `json:"timeout"`
	}
	if err = json.Unmarshal(jsonData, &input); err != nil {
		return
	}
	feed = input.Feed
	options.Since = input.Since
	options.Limit = input.Limit
	options.Conflicts = (input.Style == "all_docs")
	options.IncludeDocs = input.IncludeDocs
	filter = input.Filter

	if input.Channels != "" {
		channelsArray = strings.Split(input.Channels, ",")
	}

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
