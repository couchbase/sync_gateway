package replicator

import (
	"context"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// Used by BLIP connections for changes.  Supports both one-shot and continuous changes.
func generateBlipSyncChanges(database *db.Database, inChannels base.Set, options db.ChangesOptions, docIDFilter []string, send func([]*db.ChangeEntry) error) (err error, forceClose bool) {

	// Store one-shot here to protect
	isOneShot := !options.Continuous
	err, forceClose = GenerateChanges(context.Background(), database, inChannels, options, docIDFilter, send)

	// For one-shot changes, invoke the callback w/ nil to trigger the 'caught up' changes message.  (For continuous changes, this
	// is done by MultiChangesFeed prior to going into Wait mode)
	if isOneShot {
		_ = send(nil)
	}
	return err, forceClose
}

// Shell of the continuous changes feed -- calls out to a `send` function to deliver the change.
// This is called from BLIP connections as well as HTTP handlers, which is why this is not a
// method on `handler`.
func GenerateChanges(cancelCtx context.Context, database *db.Database, inChannels base.Set, options db.ChangesOptions, docIDFilter []string, send func([]*db.ChangeEntry) error) (err error, forceClose bool) {
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

	if options.Continuous {
		options.Wait = true // we want the feed channel to wait for changes
	}

	if !options.Since.IsNonZero() {
		database.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyPullReplicationsSinceZero, 1)
	}

	var lastSeq db.SequenceID
	var feed <-chan *db.ChangeEntry
	var timeout <-chan time.Time

	// feedStarted identifies whether at least one MultiChangesFeed has been started.  Used to identify when a one-shot changes is done.
	feedStarted := false

loop:
	for {
		// If the feed has already been started once and closed, and this isn't a continuous
		// replication, we're done.
		if feedStarted && feed == nil && !options.Continuous {
			break loop
		}

		if feed == nil {
			// Refresh the feed of all current changes:
			if lastSeq.IsNonZero() { // start after end of last feed
				options.Since = lastSeq
			}
			if database.IsClosed() {
				forceClose = true
				break loop
			}
			if len(docIDFilter) > 0 {
				feed, err = database.DocIDChangesFeed(inChannels, docIDFilter, options)
			} else {
				feed, err = database.MultiChangesFeed(inChannels, options)
			}
			if err != nil || feed == nil {
				return err, forceClose
			}
			feedStarted = true
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
				base.TracefCtx(database.Ctx, base.KeyChanges, "sending %d change(s)", len(entries))
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
			base.DebugfCtx(database.Ctx, base.KeyChanges, "heartbeat written to _changes feed for request received")
		case <-timeout:
			forceClose = true
			break loop
		case <-cancelCtx.Done():
			base.DebugfCtx(database.Ctx, base.KeyChanges, "Client connection lost")
			forceClose = true
			break loop
		case <-database.ExitChanges:
			forceClose = true
			break loop
		case <-options.Terminator:
			forceClose = true
			break loop
		}
		if err != nil {
			return err, forceClose
		}
	}

	return nil, forceClose
}
