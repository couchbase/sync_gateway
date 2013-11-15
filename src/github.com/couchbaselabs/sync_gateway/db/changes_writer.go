package db

import (
	"fmt"
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/couchbaselabs/walrus"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

// The maximum number of entries that will be kept in a ChangeLog. If the length would overflow
// this limit, the earliest/oldest entries are removed to make room.
var MaxChangeLogLength = 1000

// If this is set to true, every update of a channel log will compact it to MaxChangeLogLength.
var AlwaysCompactChangeLog = false

// Enable keeping a channel-log for the "*" channel. *ALL* revisions are written to this channel,
// which could be expensive in a busy environment. The only time this channel is needed is if
// someone has access to "*" (e.g. admin-party) and tracks its changes feed.
var EnableStarChannelLog = true

//////// CHANGES WRITER

// Coordinates writing changes to channel-log documents. A singleton owned by a DatabaseContext.
type changesWriter struct {
	bucket     base.Bucket
	logWriters map[string]*channelLogWriter
	lock       sync.Mutex
}

// Creates a new changesWriter
func newChangesWriter(bucket base.Bucket) *changesWriter {
	return &changesWriter{bucket: bucket, logWriters: map[string]*channelLogWriter{}}
}

// Adds a change to all relevant logs, asynchronously.
func (c *changesWriter) addToChangeLogs(changedChannels base.Set, channelMap ChannelMap, entry channels.LogEntry, parentRevID string) error {
	var err error
	base.LogTo("Changes", "Updating #%d %q/%q in channels %s", entry.Sequence, entry.DocID, entry.RevID, changedChannels)
	for channel, removal := range channelMap {
		if removal != nil && removal.Seq != entry.Sequence {
			continue
		}
		// Set Removed flag appropriately for this channel:
		if removal != nil {
			entry.Flags |= channels.Removed
		} else {
			entry.Flags = entry.Flags &^ channels.Removed
		}
		c.addToChangeLog(channel, entry, parentRevID)
	}

	// Finally, add to the universal "*" channel.
	if EnableStarChannelLog {
		entry.Flags = entry.Flags &^ channels.Removed
		c.addToChangeLog("*", entry, parentRevID)
	}

	return err
}

// Blocks until all pending channel-log updates are complete.
func (c *changesWriter) checkpoint() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, logWriter := range c.logWriters {
		logWriter.stop()
	}
	c.logWriters = map[string]*channelLogWriter{}
}

// Adds a change to a single channel-log (asynchronously)
func (c *changesWriter) addToChangeLog(channelName string, entry channels.LogEntry, parentRevID string) {
	c.logWriterForChannel(channelName).addChange(entry, parentRevID)
}

// Saves a channel log (asynchronously), _if_ there isn't already one in the database.
func (c *changesWriter) addChangeLog(channelName string, log *channels.ChangeLog) {
	c.logWriterForChannel(channelName).addChannelLog(log)
}

// Loads a channel's log from the database and returns it.
func (c *changesWriter) getChangeLog(channelName string, afterSeq uint64) (*channels.ChangeLog, error) {
	raw, err := c.bucket.GetRaw(channelLogDocID(channelName))
	if err != nil {
		if base.IsDocNotFoundError(err) {
			err = nil
		}
		return nil, err
	}

	log := channels.DecodeChangeLog(bytes.NewReader(raw), afterSeq)
	if log == nil {
		return nil, fmt.Errorf("Corrupt log")
	}
	base.LogTo("ChannelLog", "Read %q -- %d bytes, %d entries (since=%d) after #%d",
		channelName, len(raw), len(log.Entries), log.Since, afterSeq)
	return log, nil
}

// Internal: returns a channelLogWriter that writes to the given channel.
func (c *changesWriter) logWriterForChannel(channelName string) *channelLogWriter {
	c.lock.Lock()
	defer c.lock.Unlock()
	logWriter := c.logWriters[channelName]
	if logWriter == nil {
		logWriter = newChannelLogWriter(c.bucket, channelName)
		c.logWriters[channelName] = logWriter
	}
	return logWriter
}

//////// CHANNEL LOG WRITER

// Writes changes to a single channel log.
type channelLogWriter struct {
	bucket      base.Bucket
	channelName string
	io          chan *changeEntry
	awake       chan bool
}

type changeEntry struct {
	logEntry       *channels.LogEntry
	parentRevID    string
	replacementLog *channels.ChangeLog
}

// Max number of pending writes
const kChannelLogWriterQueueLength = 1000

// Creates a channelLogWriter for a particular channel.
func newChannelLogWriter(bucket base.Bucket, channelName string) *channelLogWriter {
	c := &channelLogWriter{
		bucket:      bucket,
		channelName: channelName,
		io:          make(chan *changeEntry, kChannelLogWriterQueueLength),
		awake:       make(chan bool, 1),
	}
	go func() {
		// This is the goroutine the channelLogWriter runs:
		for {
			if changes := c.readChanges_(); changes != nil {
				c.addToChangeLog_(c.massageChanges(changes))
				time.Sleep(50 * time.Millisecond) // lowering rate helps to coalesce changes, limiting # of writes
			} else {
				break // client called close
			}
		}
		close(c.awake)
	}()
	return c
}

// Queues a change to be written to the change-log.
func (c *channelLogWriter) addChange(entry channels.LogEntry, parentRevID string) {
	c.io <- &changeEntry{logEntry: &entry, parentRevID: parentRevID}
}

// Queues an entire new channel log to be written
func (c *channelLogWriter) addChannelLog(log *channels.ChangeLog) {
	c.io <- &changeEntry{replacementLog: log}
}

// Stops the background goroutine of a channelLogWriter.
func (c *channelLogWriter) stop() {
	close(c.io)
	<-c.awake // block until goroutine finishes
}

func (c *channelLogWriter) readChange_() *changeEntry {
	for {
		entry, ok := <-c.io
		if !ok {
			return nil
		} else if entry.replacementLog != nil {
			// Request to create the channel log if it doesn't exist:
			c.addChangeLog_(entry.replacementLog)
		} else {
			return entry
		}
	}
}

// Reads all available changes from io and returns them as an array, or nil if io is closed.
func (c *channelLogWriter) readChanges_() []*changeEntry {
	// Read first:
	entry := c.readChange_()
	if entry == nil {
		return nil
	}
	// Try to read more as long as they're available:
	entries := []*changeEntry{entry}
loop:
	for len(entries) < kChannelLogWriterQueueLength {
		var ok bool
		select {
		case entry, ok = <-c.io:
			if !ok {
				break loop
			} else if entry.replacementLog != nil {
				// Request to create the channel log if it doesn't exist:
				c.addChangeLog_(entry.replacementLog)
			} else {
				entries = append(entries, entry)
			}
		default:
			break loop
		}
	}
	return entries
}

// Simplifies an array of changes before they're appended to the channel log.
func (c *channelLogWriter) massageChanges(changes []*changeEntry) []*changeEntry {
	sort.Sort(changeEntryList(changes))
	return changes
}

// Saves a channel log, _if_ there isn't already one in the database.
func (c *channelLogWriter) addChangeLog_(log *channels.ChangeLog) (added bool, err error) {
	added, err = c.bucket.AddRaw(channelLogDocID(c.channelName), 0, encodeChannelLog(log))
	if added {
		base.LogTo("ChannelLog", "Added missing channel-log %q with %d entries",
			c.channelName, log.Len())
	} else {
		base.LogTo("ChannelLog", "Didn't add channel-log %q with %d entries (err=%v)",
			c.channelName, log.Len())
	}
	return
}

type changeEntryList []*changeEntry

func (cl changeEntryList) Len() int { return len(cl) }
func (cl changeEntryList) Less(i, j int) bool {
	return cl[i].logEntry.Sequence < cl[j].logEntry.Sequence
}
func (cl changeEntryList) Swap(i, j int) { cl[i], cl[j] = cl[j], cl[i] }

// Writes new changes to my channel log document.
func (c *channelLogWriter) addToChangeLog_(entries []*changeEntry) {
	var err error
	logDocID := channelLogDocID(c.channelName)

	// A fraction of the time we will do a full update and clean stuff out.
	fullUpdate := AlwaysCompactChangeLog || len(entries) > MaxChangeLogLength/2 || rand.Intn(MaxChangeLogLength/len(entries)) == 0
	if !fullUpdate {
		// Non-full update; just append the new entries:
		w := bytes.NewBuffer(make([]byte, 0, 100*len(entries)))
		for _, entry := range entries {
			entry.logEntry.Encode(w, entry.parentRevID)
		}
		data := w.Bytes()
		err = c.bucket.Append(logDocID, data)
		if err == nil {
			base.LogTo("ChannelLog", "Appended %d sequence(s) to %q", len(entries), c.channelName)
		} else if base.IsDocNotFoundError(err) {
			// Append failed due to doc not existing, so fall back to full update
			err = nil
			fullUpdate = true
		} else {
			base.Warn("Error appending to %q -- %v", len(entries), c.channelName, err)
		}
	}

	if fullUpdate {
		// Full update: do a CAS-based read+write:
		fullUpdateAttempts := 0
		var oldChangeLogCount, newChangeLogCount int
		err = c.bucket.WriteUpdate(logDocID, 0, func(currentValue []byte) ([]byte, walrus.WriteOptions, error) {
			fullUpdateAttempts++
			numToKeep := MaxChangeLogLength - len(entries)
			if len(currentValue) == 0 || numToKeep <= 0 {
				// If log was missing or empty, or will be entirely overwritten, create a new one:
				entriesToWrite := entries
				if numToKeep < 0 {
					entriesToWrite = entries[-numToKeep:]
				}
				channelLog := channels.ChangeLog{}
				for _, entry := range entriesToWrite {
					channelLog.Add(*entry.logEntry)
				}
				newChangeLogCount = len(entriesToWrite)
				oldChangeLogCount = newChangeLogCount
				return encodeChannelLog(&channelLog), walrus.Raw, nil
			} else {
				// Append to an already existing change log:
				var newValue bytes.Buffer
				var nRemoved int
				nRemoved, newChangeLogCount = channels.TruncateEncodedChangeLog(
					bytes.NewReader(currentValue), numToKeep, numToKeep/2, &newValue)
				for _, entry := range entries {
					entry.logEntry.Encode(&newValue, entry.parentRevID)
				}
				oldChangeLogCount = nRemoved + newChangeLogCount
				newChangeLogCount += len(entries)
				return newValue.Bytes(), walrus.Raw, nil
			}
		})
		if err == nil {
			base.LogTo("ChannelLog", "Wrote %d sequences (was %d now %d) to %q in %d attempts",
				len(entries), oldChangeLogCount, newChangeLogCount, c.channelName, fullUpdateAttempts)
		} else {
			base.Warn("Error writing %d sequence(s) to %q -- %v", len(entries), c.channelName, err)
		}
	}
}

//////// SUBROUTINES:

// The "2" is a version tag. Update this if we change the format later.
const kChannelLogDocType = "log2"
const kChannelLogKeyPrefix = "_sync:" + kChannelLogDocType + ":"

func channelLogDocID(channelName string) string {
	return kChannelLogKeyPrefix + channelName
}

func encodeChannelLog(log *channels.ChangeLog) []byte {
	if log == nil {
		return nil
	}
	raw := bytes.NewBuffer(make([]byte, 0, 50000))
	log.Encode(raw)
	return raw.Bytes()
}
