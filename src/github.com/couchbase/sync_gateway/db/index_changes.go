//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"fmt"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// Returns the (ordered) union of all of the changes made to multiple channels.
func (db *Database) VectorMultiChangesFeed(chans base.Set, options ChangesOptions) (<-chan *ChangeEntry, error) {
	to := ""
	if db.user != nil && db.user.Name() != "" {
		to = fmt.Sprintf("  (to %s)", db.user.Name())
	}

	base.LogTo("DIndex+", "Vector MultiChangesFeed(%s, %+v) ... %s", chans, options, to)
	base.LogTo("DIndex+", "Vector MultiChangesFeed since:%s", base.PrintClock(options.Since.Clock))
	output := make(chan *ChangeEntry, 50)

	go func() {
		defer func() {
			base.LogTo("DIndex+", "MultiChangesFeed done %s", to)
			close(output)
		}()

		var changeWaiter *changeWaiter
		var userChangeCount uint64
		var addedChannels base.Set // Tracks channels added to the user during changes processing.

		if options.Wait {
			// Note (Adam): I don't think there's a reason to set this to false here.  We're outside the
			// main iteration loop (so the if check above should only happen once), and I don't believe
			// options.Wait is referenced elsewhere once MultiChangesFeed is called.  Leaving it as-is
			// makes it possible for channels to identify whether a getChanges call has options.Wait set to true,
			// which is useful to identify active change listeners.  However, it's possible there's a subtlety of
			// longpoll or continuous processing I'm missing here - leaving this note instead of just deleting for now.
			//options.Wait = false
			changeWaiter = db.startChangeWaiter(chans)
			userChangeCount = changeWaiter.CurrentUserCount()
		}
		cumulativeClock := options.Since.Clock

		// This loop is used to re-run the fetch after every database change, in Wait mode
	outer:
		for {
			// Restrict to available channels, expand wild-card, and find since when these channels
			// have been available to the user:
			var channelsSince channels.TimedSet
			if db.user != nil {
				channelsSince = db.user.FilterToAvailableChannels(chans)
			} else {
				channelsSince = channels.AtSequence(chans, 0)
			}

			// Updates the changeWaiter to the current set of available channels
			if changeWaiter != nil {
				changeWaiter.UpdateChannels(channelsSince)
			}
			base.LogTo("Changes+", "MultiChangesFeed: channels expand to %#v ... %s", channelsSince, to)

			// Populate the  array of feed channels:
			feeds := make([]<-chan *ChangeEntry, 0, len(channelsSince))

			base.LogTo("DIndex+", "GotChannelSince... %v", channelsSince)
			for name, seqAddedAt := range channelsSince {

				base.LogTo("DIndex+", "Starting for channel... %s", name)
				chanOpts := options

				// Check whether requires backfill based on addedChannels in this _changes feed
				isNewChannel := false
				if addedChannels != nil {
					_, isNewChannel = addedChannels[name]
				}

				// Check whether requires backfill based on current sequence, seqAddedAt
				// Triggered by handling:
				//   1. options.Since.TriggeredBy == seqAddedAt : We're in the middle of backfill for this channel, based
				//    on the access grant in sequence options.Since.TriggeredBy.  Normally the entire backfill would be done in one
				//    changes feed iteration, but this can be split over multiple iterations when 'limit' is used.
				//   2. options.Since.TriggeredBy == 0 : Not currently doing a backfill
				//   3. options.Since.TriggeredBy != 0 and <= seqAddedAt: We're in the middle of a backfill for another channel, but the backfill for
				//     this channel is still pending.  Initiate the backfill for this channel - will be ordered below in the usual way (iterating over all channels)

				// Backfill required when seqAddedAt is before current sequence
				backfillRequired := seqAddedAt > 1 && options.Since.Before(SequenceID{Seq: seqAddedAt})

				// Ensure backfill isn't already in progress for this seqAddedAt
				backfillPending := options.Since.TriggeredBy == 0 || options.Since.TriggeredBy < seqAddedAt

				if isNewChannel || (backfillRequired && backfillPending) {
					// Newly added channel so initiate backfill:
					chanOpts.Since = SequenceID{Seq: 0, TriggeredBy: seqAddedAt}
				}
				feed, err := db.vectorChangesFeed(name, chanOpts)
				if err != nil {
					base.Warn("MultiChangesFeed got error reading changes feed %q: %v", name, err)
					return
				}
				feeds = append(feeds, feed)
			}

			// If the user object has changed, create a special pseudo-feed for it:
			if db.user != nil {
				feeds, _ = db.appendVectorUserFeed(feeds, []string{}, options)
			}

			current := make([]*ChangeEntry, len(feeds))

			// This loop reads the available entries from all the feeds in parallel, merges them,
			// and writes them to the output channel:
			var sentSomething bool
			for {
				// Read more entries to fill up the current[] array:
				for i, cur := range current {
					if cur == nil && feeds[i] != nil {
						var ok bool
						current[i], ok = <-feeds[i]
						if !ok {
							feeds[i] = nil
						}
					}
				}

				// Find the current entry with the minimum sequence:
				minSeq := MaxSequenceID
				var minEntry *ChangeEntry
				for _, cur := range current {
					if cur != nil && cur.Seq.Before(minSeq) {
						minSeq = cur.Seq
						minEntry = cur
					}
				}

				if minEntry == nil {
					break // Exit the loop when there are no more entries
				}

				// Clear the current entries for the sequence just sent:
				for i, cur := range current {
					if cur != nil && cur.Seq == minSeq {
						current[i] = nil
						// Also concatenate the matching entries' Removed arrays:
						if cur != minEntry && cur.Removed != nil {
							if minEntry.Removed == nil {
								minEntry.Removed = cur.Removed
							} else {
								minEntry.Removed = minEntry.Removed.Union(cur.Removed)
							}
						}
					}
				}

				// Update options.Since for use in the next outer loop iteration.
				options.Since.Clock = cumulativeClock

				// Add the doc body or the conflicting rev IDs, if those options are set:
				if options.IncludeDocs || options.Conflicts {
					db.addDocToChangeEntry(minEntry, options)
				}

				// update the cumulative clock, and stick it on the entry
				cumulativeClock.SetMaxSequence(minEntry.Seq.vbNo, minEntry.Seq.Seq)
				minEntry.Seq.Clock = cumulativeClock
				clockHash, err := db.SequenceHasher.GetHash(minEntry.Seq.Clock)
				minEntry.Seq.Clock = &base.SequenceClockImpl{}
				base.LogTo("DIndex+", "calculated hash...%v", clockHash)
				if err != nil {
					base.Warn("Error calculating hash for clock:%v", base.PrintClock(minEntry.Seq.Clock))
				} else {
					minEntry.Seq.Clock.SetHashedValue(clockHash)
				}

				// Send the entry, and repeat the loop:
				select {
				case <-options.Terminator:
					return
				case output <- minEntry:
				}
				sentSomething = true

				// Stop when we hit the limit (if any):
				if options.Limit > 0 {
					options.Limit--
					if options.Limit == 0 {
						break outer
					}
				}
			}

			if !options.Continuous && (sentSomething || changeWaiter == nil) {
				break
			}

			// If nothing found, and in wait mode: wait for the db to change, then run again.
			// First notify the reader that we're waiting by sending a nil.
			base.LogTo("Changes+", "MultiChangesFeed waiting... %s", to)
			output <- nil
			if !changeWaiter.Wait() {
				break
			}

			// Check whether I was terminated while waiting for a change:
			select {
			case <-options.Terminator:
				return
			default:
			}

			// Before checking again, update the User object in case its channel access has
			// changed while waiting:
			var err error
			userChangeCount, addedChannels, err = db.checkForUserUpdates(userChangeCount, changeWaiter)
			if err != nil {
				change := makeErrorEntry("User not found during reload - terminating changes feed")
				base.LogTo("Changes+", "User not found during reload - terminating changes feed with entry %+v", change)
				output <- &change
				return
			}

		}
	}()

	return output, nil
}

// Creates a Go-channel of all the changes made on a channel.
// Does NOT handle the Wait option. Does NOT check authorization.
func (db *Database) vectorChangesFeed(channel string, options ChangesOptions) (<-chan *ChangeEntry, error) {
	dbExpvars.Add("channelChangesFeeds", 1)
	log, err := db.changeCache.GetChanges(channel, options)
	base.LogTo("DIndex+", "[changesFeed] Found %d changes for channel %s", len(log), channel)
	if err != nil {
		return nil, err
	}

	if len(log) == 0 {
		// There are no entries newer than 'since'. Return an empty feed:
		feed := make(chan *ChangeEntry)
		close(feed)
		return feed, nil
	}

	feed := make(chan *ChangeEntry, 1)
	go func() {
		defer close(feed)
		// Now write each log entry to the 'feed' channel in turn:
		for _, logEntry := range log {
			base.LogTo("DIndex+", "vectorChangesFeed, adding entry for [%v,%v]", logEntry.VbNo, logEntry.Sequence)
			if logEntry.Sequence >= options.Since.TriggeredBy {
				options.Since.TriggeredBy = 0
			}
			seqID := SequenceID{
				SeqType:          ClockSequenceType,
				Seq:              logEntry.Sequence,
				vbNo:             logEntry.VbNo,
				TriggeredByClock: options.Since.TriggeredByClock,
			}

			change := makeChangeEntry(logEntry, seqID, channel)

			select {
			case <-options.Terminator:
				base.LogTo("Changes+", "Aborting changesFeed")
				return
			case feed <- &change:
			}
		}
	}()
	return feed, nil
}

func (db *Database) appendVectorUserFeed(feeds []<-chan *ChangeEntry, names []string, options ChangesOptions) ([]<-chan *ChangeEntry, []string) {
	userSeq := SequenceID{Seq: db.user.Sequence()}
	if options.Since.Before(userSeq) {
		name := db.user.Name()
		if name == "" {
			name = base.GuestUsername
		}
		entry := ChangeEntry{
			Seq:     userSeq,
			ID:      "_user/" + name,
			Changes: []ChangeRev{},
		}
		userFeed := make(chan *ChangeEntry, 1)
		userFeed <- &entry
		close(userFeed)
		feeds = append(feeds, userFeed)
		names = append(names, entry.ID)
	}
	return feeds, names
}
