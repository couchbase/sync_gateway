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
	var userVbNo uint16
	if db.user != nil && db.user.Name() != "" {
		to = fmt.Sprintf("  (to %s)", db.user.Name())
		userVbNo = uint16(db.Bucket.VBHash(db.user.DocID()))
	}

	base.LogTo("Changes+", "Vector MultiChangesFeed(%s, %+v) ... %s", chans, options, to)
	output := make(chan *ChangeEntry, 50)

	go func() {
		var cumulativeClock *base.SyncSequenceClock
		var lastHashedValue string
		hashedEntryCount := 0
		defer func() {
			base.LogTo("Changes+", "MultiChangesFeed done %s", to)
			close(output)
		}()

		var changeWaiter *changeWaiter
		var userChangeCount uint64
		var addedChannels base.Set // Tracks channels added to the user during changes processing.

		if options.Wait {
			changeWaiter = db.startChangeWaiter(chans)
			userChangeCount = changeWaiter.CurrentUserCount()
			db.initializePrincipalPolling(changeWaiter.GetUserKeys())
		}

		cumulativeClock = base.NewSyncSequenceClock()
		cumulativeClock.SetTo(getChangesClock(options.Since))

		// This loop is used to re-run the fetch after every database change, in Wait mode
	outer:
		for {
			// Get the last polled stable sequence.  We don't return anything later than stable sequence in each iteration
			stableClock, err := db.changeCache.GetStableClock(true)
			if err != nil {
				base.Warn("MultiChangesFeed got error reading stable sequence: %v", err)
				return
			}

			// Restrict to available channels, expand wild-card, and find since when these channels
			// have been available to the user:
			var channelsSince channels.TimedSet
			if db.user != nil {
				channelsSince = db.user.FilterToAvailableChannels(chans)
			} else {
				channelsSince = channels.AtSequence(chans, 0)
			}

			// Updates the changeWaiter to the current set of available channels.
			if changeWaiter != nil {
				changeWaiter.UpdateChannels(channelsSince)
			}
			base.LogTo("Changes+", "MultiChangesFeed: channels expand to %#v ... %s", channelsSince, to)

			// Build the channel feeds.
			feeds, err := db.initializeChannelFeeds(channelsSince, options, addedChannels, userVbNo)
			if err != nil {
				return
			}

			// This loop reads the available entries from all the feeds in parallel, merges them,
			// and writes them to the output channel:
			current := make([]*ChangeEntry, len(feeds))
			var sentSomething bool
			nextEntry := getNextSequenceFromFeeds(current, feeds)
			for {
				minEntry := nextEntry

				if minEntry == nil {
					break // Exit the loop when there are no more entries
				}

				// Calculate next entry here, to help identify whether minEntry is the last entry we're sending,
				// to guarantee hashing
				nextEntry = getNextSequenceFromFeeds(current, feeds)

				if options.ActiveOnly {
					if minEntry.Deleted || minEntry.allRemoved {
						continue
					}
				}

				// Don't send any entries later than the stable sequence
				if stableClock.GetSequence(minEntry.Seq.vbNo) < minEntry.Seq.Seq {
					continue
				}

				// Add the doc body or the conflicting rev IDs, if those options are set:
				if options.IncludeDocs || options.Conflicts {
					db.addDocToChangeEntry(minEntry, options)
				}

				// Clock handling
				if minEntry.Seq.TriggeredBy == 0 {
					// Update the cumulative clock, and stick it on the entry.
					cumulativeClock.SetMaxSequence(minEntry.Seq.vbNo, minEntry.Seq.Seq)
					// Force new hash generation for non-continuous changes feeds if this is the last entry to be sent - either
					// because there are no more entries in the channel feeds, or we're going to hit the limit.
					forceHash := false
					if options.Continuous == false && (nextEntry == nil || options.Limit == 1) {
						forceHash = true
					}
					lastHashedValue = db.calculateHashWhenNeeded(
						options,
						minEntry,
						cumulativeClock,
						&hashedEntryCount,
						lastHashedValue,
						forceHash,
					)

				} else {
					// For backfill (triggered by), we don't want to update the cumulative clock.  All entries triggered by the
					// same sequence reference the same triggered by clock, so it should only need to get hashed once.
					// If this is the first entry for this triggered by, initialize the triggered by clock's
					// hash value.
					if minEntry.Seq.TriggeredByClock.GetHashedValue() == "" {
						cumulativeClock.SetMaxSequence(minEntry.Seq.TriggeredByVbNo, minEntry.Seq.TriggeredBy)
						clockHash, err := db.SequenceHasher.GetHash(cumulativeClock)
						if err != nil {
							base.Warn("Error calculating hash for triggered by clock:%v", base.PrintClock(cumulativeClock))
						} else {
							minEntry.Seq.TriggeredByClock.SetHashedValue(clockHash)
						}
					}
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

			// Update options.Since for use in the next outer loop iteration.
			options.Since.Clock = cumulativeClock

			// If nothing found, and in wait mode: wait for the db to change, then run again.
			// First notify the reader that we're waiting by sending a nil.
			base.LogTo("Changes+", "MultiChangesFeed waiting... %s", to)
			output <- nil

		waitForChanges:
			for {
				waitResponse := changeWaiter.Wait()
				if waitResponse == WaiterClosed {
					break outer
				} else if waitResponse == WaiterHasChanges {
					select {
					case <-options.Terminator:
						return
					default:
						break waitForChanges
					}
				} else if waitResponse == WaiterCheckTerminated {
					// Check whether I was terminated while waiting for a change.  If not, resume wait.
					select {
					case <-options.Terminator:
						return
					default:
					}
				}
			}

			// Before checking again, update the User object in case its channel access has
			// changed while waiting:
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

// Gets the next sequence from the set of feeds, including handling for sequences appearing in multiple feeds.
func getNextSequenceFromFeeds(current []*ChangeEntry, feeds []<-chan *ChangeEntry) (minEntry *ChangeEntry) {
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
	for _, cur := range current {
		if cur != nil && cur.Seq.Before(minSeq) {
			minSeq = cur.Seq
			minEntry = cur
		}
	}

	if minEntry == nil {
		return nil // No more entries
	}

	if minEntry.Removed != nil {
		minEntry.allRemoved = true
	}

	// Clear the current entries for any duplicates of the sequence just sent:
	for i, cur := range current {
		if cur != nil && cur.Seq == minSeq {
			current[i] = nil
			// Track whether this is a removal from all user's channels
			if cur.Removed == nil && minEntry.allRemoved == true {
				minEntry.allRemoved = false
			}
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
	return minEntry
}

// Determines whether the clock hash should be calculated for the entry. For non-continuous changes feeds, hash is only calculated for
// the last entry sent (for use in last_seq), and is done in the defer for the main VectorMultiChangesFeed.
// For continuous changes feeds, we want to calculate the hash for every nth entry, where n=kChangesHashFrequency.  To ensure that
// clients can restart a new changes feed based on any sequence in the continuous feed, we set the last hash calculated as the LowHash
// value on the sequence.
func (db *Database) calculateHashWhenNeeded(options ChangesOptions, entry *ChangeEntry, cumulativeClock base.SequenceClock, hashedEntryCount *int, lastHashedValue string, forceHash bool) string {

	// When hashedEntryCount == 0 or forceHash==true recalculate hash
	if *hashedEntryCount == 0 || forceHash {
		clockHash, err := db.SequenceHasher.GetHash(cumulativeClock)
		if err != nil {
			base.Warn("Error calculating hash for clock:%v", base.PrintClock(cumulativeClock))
			return lastHashedValue
		} else {
			entry.Seq.Clock = base.NewSyncSequenceClock()
			entry.Seq.Clock.SetHashedValue(clockHash)
			lastHashedValue = clockHash
		}
		*hashedEntryCount = db.SequenceHasher.getHashFrequency()
	} else {
		entry.Seq.LowHash = lastHashedValue
		*hashedEntryCount--
	}
	return lastHashedValue
}

// Creates a go-channel of ChangeEntry for each channel in channelsSince.  Each go-channel sends the ordered entries for that channel.
func (db *Database) initializeChannelFeeds(channelsSince channels.TimedSet, options ChangesOptions, addedChannels base.Set, userVbNo uint16) ([]<-chan *ChangeEntry, error) {
	// Populate the  array of feed channels:
	feeds := make([]<-chan *ChangeEntry, 0, len(channelsSince))

	base.LogTo("Changes+", "GotChannelSince... %v", channelsSince)
	for name, vbSeqAddedAt := range channelsSince {
		seqAddedAt := vbSeqAddedAt.Sequence
		// If there's no vbNo on the channelsSince, it indicates a user doc channel grant - use the userVbNo.
		var vbAddedAt uint16
		if vbSeqAddedAt.VbNo == nil {
			vbAddedAt = userVbNo
		} else {
			vbAddedAt = *vbSeqAddedAt.VbNo
		}

		base.LogTo("Changes+", "Starting for channel... %s, %d", name, seqAddedAt)
		chanOpts := options

		// Check whether requires backfill based on addedChannels in this _changes feed
		isNewChannel := false
		if addedChannels != nil {
			_, isNewChannel = addedChannels[name]
		}

		// Three possible scenarios for backfill handling, based on whether the incoming since value indicates a backfill in progress
		// for this channel, and whether the channel requires a new backfill to be started
		//   Case 1. No backfill in progress, no backfill required - use the incoming since to get changes
		//   Case 2. No backfill in progress, backfill required for this channel.  Get changes since zero, backfilling to the incoming since
		//   Case 3. Backfill in progress.  Get changes since zero, backfilling to incoming triggered by, filtered to later than incoming since.
		backfillInProgress := false
		if options.Since.TriggeredByClock != nil {
			// There's a backfill in progress for SOME channel - check if it's this one
			if options.Since.TriggeredByClock.GetSequence(vbAddedAt) == seqAddedAt {
				backfillInProgress = true
			}
		}

		sinceSeq := getChangesClock(options.Since).GetSequence(vbAddedAt)
		backfillRequired := vbSeqAddedAt.Sequence > 0 && sinceSeq < seqAddedAt

		if isNewChannel || (backfillRequired && !backfillInProgress) {
			// Case 2.  No backfill in progress, backfill required
			base.LogTo("Changes+", "Starting backfill for channel... %s, %d", name, seqAddedAt)
			chanOpts.Since = SequenceID{
				Seq:              0,
				vbNo:             0,
				Clock:            base.NewSequenceClockImpl(),
				TriggeredBy:      seqAddedAt,
				TriggeredByVbNo:  vbAddedAt,
				TriggeredByClock: getChangesClock(options.Since).Copy(),
			}
		} else if backfillInProgress {
			// Case 3.  Backfill in progress.
			chanOpts.Since = SequenceID{
				Seq:              options.Since.Seq,
				vbNo:             options.Since.vbNo,
				Clock:            base.NewSequenceClockImpl(),
				TriggeredBy:      seqAddedAt,
				TriggeredByVbNo:  vbAddedAt,
				TriggeredByClock: options.Since.TriggeredByClock,
			}
		} else {
			// Case 1.  Leave chanOpts.Since set to options.Since.
		}
		feed, err := db.vectorChangesFeed(name, chanOpts)
		if err != nil {
			base.Warn("MultiChangesFeed got error reading changes feed %q: %v", name, err)
			return feeds, err
		}
		feeds = append(feeds, feed)
	}

	// If the user object has changed, create a special pseudo-feed for it:
	if db.user != nil {
		feeds, _ = db.appendVectorUserFeed(feeds, []string{}, options, userVbNo)
	}
	return feeds, nil
}

// Creates a Go-channel of all the changes made on a channel.
// Does NOT handle the Wait option. Does NOT check authorization.
func (db *Database) vectorChangesFeed(channel string, options ChangesOptions) (<-chan *ChangeEntry, error) {
	dbExpvars.Add("channelChangesFeeds", 1)
	log, err := db.changeCache.GetChanges(channel, options)
	base.LogTo("Changes+", "[changesFeed] Found %d changes for channel %s", len(log), channel)
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

		// Send backfill first
		if options.Since.TriggeredByClock != nil {
			for i := 0; i < len(log); i++ {
				logEntry := log[i]
				// If sequence is less than the backfillTo clock sequence for its vbucket, send as backfill (i.e. with triggered by)
				isBackfill := logEntry.Sequence <= options.Since.TriggeredByClock.GetSequence(logEntry.VbNo)

				// Only send backfill that's hasn't already been sent (i.e. after the sequence part of options.Since)
				isPending := options.Since.VbucketSequenceBefore(logEntry.VbNo, logEntry.Sequence)

				if isBackfill && isPending {
					seqID := SequenceID{
						SeqType:          ClockSequenceType,
						Seq:              logEntry.Sequence,
						vbNo:             logEntry.VbNo,
						TriggeredBy:      options.Since.TriggeredBy,
						TriggeredByVbNo:  options.Since.TriggeredByVbNo,
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
				if isBackfill {
					// remove from the set, so that it's not resent below
					log[i] = nil
				}
			}
		}

		// Now send any remaining entries
		for _, logEntry := range log {
			// Ignore any already sent as backfill
			if logEntry != nil {
				seqID := SequenceID{
					SeqType: ClockSequenceType,
					Seq:     logEntry.Sequence,
					vbNo:    logEntry.VbNo,
				}
				change := makeChangeEntry(logEntry, seqID, channel)
				select {
				case <-options.Terminator:
					base.LogTo("Changes+", "Aborting changesFeed")
					return
				case feed <- &change:
				}
			}
		}
	}()
	return feed, nil
}

func (db *Database) appendVectorUserFeed(feeds []<-chan *ChangeEntry, names []string, options ChangesOptions, userVbNo uint16) ([]<-chan *ChangeEntry, []string) {

	if db.user.Sequence() > 0 {
		userSeq := SequenceID{
			SeqType: ClockSequenceType,
			Seq:     db.user.Sequence(),
			vbNo:    userVbNo,
		}

		// Get since sequence for the userSeq's vbucket
		sinceSeq := getChangesClock(options.Since).GetSequence(userVbNo)

		if sinceSeq < userSeq.Seq {
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
	}
	return feeds, names
}

// Adds the specified user keys to the set used during polling.
func (db *Database) initializePrincipalPolling(userKeys []string) {
	if len(userKeys) == 0 {
		return
	}
	changeIndex, ok := db.changeCache.(*kvChangeIndex)
	if !ok {
		return
	}
	changeIndex.reader.addActivePrincipals(userKeys)
}

func getChangesClock(sequence SequenceID) base.SequenceClock {
	if sequence.TriggeredByClock != nil {
		return sequence.TriggeredByClock
	} else {
		return sequence.Clock
	}
}
