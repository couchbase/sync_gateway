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
	"strings"
	"time"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
	"github.com/couchbaselabs/sync_gateway/db"
)

func (h *handler) handleRevsDiff() error {
	var input db.RevsDiffInput
	err := db.ReadJSONFromMIME(h.rq.Header, h.rq.Body, &input)
	if err != nil {
		return err
	}
	output, err := h.db.RevsDiff(input)
	if err == nil {
		h.writeJSON(output)
	}
	return err
}

func (h *handler) handleChanges() error {
	// http://wiki.apache.org/couchdb/HTTP_database_API#Changes
	var options db.ChangesOptions
	options.Since = channels.TimedSetFromString(h.getQuery("since"))
	options.Limit = int(h.getIntQuery("limit", 0))
	options.Conflicts = (h.getQuery("style") == "all_docs")
	options.IncludeDocs = (h.getBoolQuery("include_docs"))

	// Get the channels as parameters to an imaginary "bychannel" filter.
	// The default is all channels the user can access.
	userChannels := channels.SetOf("*")
	filter := h.getQuery("filter")
	if filter != "" {
		if filter != "sync_gateway/bychannel" {
			return &base.HTTPError{http.StatusBadRequest, "Unknown filter; try sync_gateway/bychannel"}
		}
		channelsParam := h.getQuery("channels")
		if channelsParam == "" {
			return &base.HTTPError{http.StatusBadRequest, "Missing 'channels' filter parameter"}
		}
		var err error
		userChannels, err = channels.SetFromArray(strings.Split(channelsParam, ","),
			channels.ExpandStar)
		if err != nil {
			return err
		}
		if len(userChannels) == 0 {
			return &base.HTTPError{http.StatusBadRequest, "Empty channel list"}
		}
	}

	switch h.getQuery("feed") {
	case "continuous":
		return h.handleContinuousChanges(userChannels, options)
	case "longpoll":
		options.Wait = true
	}
	return h.handleSimpleChanges(userChannels, options)
}

func (h *handler) handleSimpleChanges(channels base.Set, options db.ChangesOptions) error {
	var lastSeqID string
	var first bool = true
	feed, err := h.db.MultiChangesFeed(channels, options)
	if err == nil {
		h.setHeader("Content-Type", "text/plain; charset=utf-8")
		h.writeln([]byte("{\"results\":["))
		if feed != nil {
			for entry := range feed {
				str, _ := json.Marshal(entry)
				var buf []byte
				if first {
					first = false
					buf = str
				} else {
					buf = []byte{','}
					buf = append(buf, str...)
				}
				if err = h.writeln(buf); err != nil {
					err = nil
					break
				}
				lastSeqID = entry.Seq
			}
		}
		s := fmt.Sprintf("],\n\"last_seq\":%q}", lastSeqID)
		h.writeln([]byte(s))
	}
	return err
}

func (h *handler) handleContinuousChanges(inChannels base.Set, options db.ChangesOptions) error {
	var timeoutInterval time.Duration
	var timer *time.Timer
	var heartbeat <-chan time.Time
	if ms := h.getIntQuery("heartbeat", 0); ms > 0 {
		ticker := time.NewTicker(time.Duration(ms) * time.Millisecond)
		defer ticker.Stop()
		heartbeat = ticker.C
	} else if ms := h.getIntQuery("timeout", 60000); ms > 0 {
		timeoutInterval = time.Duration(ms) * time.Millisecond
		defer func() {
			if timer != nil {
				timer.Stop()
			}
		}()
	}

	options.Wait = true // we want the feed channel to wait for changes
	var lastSeqID string
	var feed <-chan *db.ChangeEntry
	var timeout <-chan time.Time
	var err error

loop:
	for {
		if feed == nil {
			// Refresh the feed of all current changes:
			if lastSeqID != "" { // start after end of last feed
				options.Since = channels.TimedSetFromString(lastSeqID)
			}
			feed, err = h.db.MultiChangesFeed(inChannels, options)
			if err != nil || feed == nil {
				return err
			}
		}

		if timeoutInterval > 0 && timer == nil {
			timer = time.NewTimer(timeoutInterval)
			timeout = timer.C
		}

		// Wait for either a new change, a heartbeat, or a timeout:
		select {
		case entry := <-feed:
			if entry == nil {
				feed = nil
			} else {
				str, _ := json.Marshal(entry)
				base.LogTo("Changes", "send change: %s", str)
				err = h.writeln(str)

				lastSeqID = entry.Seq
				if options.Limit > 0 {
					options.Limit--
					if options.Limit == 0 {
						break loop
					}
				}
			}
			// Reset the timeout after sending an entry:
			if timer != nil {
				timer.Stop()
				timer = nil
			}
		case <-heartbeat:
			err = h.writeln([]byte{})
		case <-timeout:
			break loop
		}

		if err != nil {
			return nil // error is probably because the client closed the connection
		}
	}
	return nil
}
