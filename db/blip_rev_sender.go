/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// A queue that takes `revToSend` structs that define a revision to send to the peer,
// and sends those to BlipSyncContext's `sendRevision` and `sendRevAsDelta` methods at a rate
// that ensures only a limited number of outgoing in-memory BLIP "rev" messages are present at once.
type blipRevSender struct {
	bsc            *BlipSyncContext // The main sync object [const]
	maxActiveCount int              // Max number of messages I can be sending at once [const]
	maxActiveBytes int64            // Max total size of messages I'm sending [const]
	mutex          sync.Mutex       // Synchronizes access to queue,activeCount
	queue          []*revToSend     // Ordered queue of revisions to be sent [synced]
	activeCount    int              // Number of revs being fetched, processed, sent [synced]
	sendingCountA  int64            // Number of BLIP messages being sent to the socket [atomic]
	sendingBytesA  int64            // Total size of BLIP messages I'm sending [atomic]
}

// Captures the information about a "rev" message to send. Queued by blipRevSender.
type revToSend struct {
	seq           SequenceID   // Sequence
	docID         string       // Document ID to send
	revID         string       // Revision ID to send
	knownRevs     []any        // RevIDs the client already has
	maxHistory    int          // Max length of rev history to send
	useDelta      bool         // If true, send as delta if possible
	collectionIdx *int         // Identifies which collection
	sender        *blip.Sender // BLIP sender
	timestamp     time.Time    // When the 'changes' response was received
	messageLen    int          // Length of BLIP message; must be filled in when message sent
}

// Creates a new blipRevSender.
//   - `maxActiveCount` is the maximum number of revisions that can be actively processed:
//     fetched from the database, converted to 'rev' messages, and being written to the socket.
//   - `maxActiveBytes` is the (approximate) maximum total size in bytes of those messages,
//     or 0 for no size limit.
func newBlipRevSender(blipSyncContext *BlipSyncContext, maxActiveCount int, maxActiveBytes int64) *blipRevSender {
	return &blipRevSender{
		bsc:            blipSyncContext,
		maxActiveCount: maxActiveCount,
		maxActiveBytes: maxActiveBytes,
	}
}

// Queues revisions to send.
func (s *blipRevSender) addRevs(revs []*revToSend) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queue = append(s.queue, revs...)
	s._sendMore()
}

// To be called by the BlipSyncContext when it's finished writing a 'rev' message to the socket.
func (s *blipRevSender) completedRev(rev *revToSend) {
	messageLen := rev.messageLen
	s.bsc.threadPool.Go(func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()

		s.activeCount--
		atomic.AddInt64(&s.sendingCountA, int64(-1))
		atomic.AddInt64(&s.sendingBytesA, int64(-messageLen))
		s._sendMore()
	})
}

func (s *blipRevSender) _sendMore() {
	// Mutex must be locked when calling this!

	// Get the current total size, and estimate the size of a message:
	curSendingCount := atomic.LoadInt64(&s.sendingCountA)
	estSendingBytes := atomic.LoadInt64(&s.sendingBytesA)
	var estMessageSize int64 = 4096
	if curSendingCount > 0 {
		estMessageSize = estSendingBytes / curSendingCount
	}

	n := 0
	for s.activeCount < s.maxActiveCount && len(s.queue) > 0 {
		if s.maxActiveBytes > 0 && estSendingBytes+estMessageSize > s.maxActiveBytes {
			// Stop if the byte count is too high
			break
		}
		// Send the next revision (asynchronously):
		next := s.queue[0]
		s.queue = s.queue[1:]
		s.activeCount++
		s.bsc.threadPool.Go(func() { s._sendNow(next) })
		estSendingBytes += estMessageSize
		n++
	}
	// if len(s.queue) > 0 {
	// 	base.WarnfCtx(s.bsc.loggingCtx, "_sendMore: stopping after %d, at %d bytes (est), %d messages ... avg msg size is %d", n, estSendingBytes, s.activeCount, estMessageSize)
	// }
}

func (s *blipRevSender) _sendNow(rev *revToSend) {
	// Sends a 'rev' message, or if that fails, sends a 'norev'; then updates stats.
	if err := s._trySendNow(rev); err != nil {
		if base.IsDocNotFoundError(err) {
			// If rev isn't available, send a 'norev'. This is important for client bookkeeping.
			err = s.bsc.sendNoRev(rev, err)
		}
		if err != nil {
			base.ErrorfCtx(s.bsc.loggingCtx, "Error sending 'rev' over BLIP: %s", err)
			if cb := s.bsc.fatalErrorCallback; cb != nil {
				cb(err)
			}
		}
	}

	atomic.AddInt64(&s.sendingCountA, int64(1))
	atomic.AddInt64(&s.sendingBytesA, int64(rev.messageLen))

	latency := time.Since(rev.timestamp).Nanoseconds()
	s.bsc.replicationStats.HandleChangesSendRevCount.Add(1)
	s.bsc.replicationStats.HandleChangesSendRevLatency.Add(latency)
}

func (s *blipRevSender) _trySendNow(rev *revToSend) error {
	// Sends a 'rev' message or returns an error. (Subroutine of _sendNow.)

	// Convert knownRevs to a set of strings:
	knownRevs := make(map[string]bool, len(rev.knownRevs))
	var deltaSrcRevID *string
	for _, knownRev := range rev.knownRevs {
		if revID, ok := knownRev.(string); ok {
			knownRevs[revID] = true
			if deltaSrcRevID == nil {
				// The first element of the knownRevs array is the one to use as deltaSrc
				deltaSrcRevID = &revID
			}
		} else {
			base.ErrorfCtx(s.bsc.loggingCtx, "Invalid knownRevs in response to 'changes' message")
		}
	}
	rev.knownRevs = nil // (no longer needed)

	collection, err := s.bsc.copyDatabaseCollectionWithUser(rev.collectionIdx)
	if err != nil {
		return err
	}
	if rev.useDelta && deltaSrcRevID != nil {
		sent, err := s.bsc.sendRevAsDelta(collection, rev, knownRevs, *deltaSrcRevID)
		if sent || err != nil {
			return err
		}
		// if rev can't be sent as a delta, send it as a full revision...
	}
	return s.bsc.sendRevision(collection, rev, knownRevs)
}
