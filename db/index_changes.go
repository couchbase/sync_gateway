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
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

const (
	backfillPendingPrefix  = "_sync:backfill:pending:"
	backfillCompletePrefix = "_sync:backfill:complete:"
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
		var userCounter uint64     // Wait counter used to identify changes to the user document
		var addedChannels base.Set // Tracks channels added to the user during changes processing.
		var userChanged bool       // Whether the user document has changed

		// Restrict to available channels, expand wild-card, and find since when these channels
		// have been available to the user:
		var channelsSince, secondaryTriggers channels.TimedSet
		if db.user != nil {
			channelsSince, secondaryTriggers = db.user.FilterToAvailableChannelsForSince(chans, getChangesClock(options.Since))
		} else {
			channelsSince = channels.AtSequence(chans, 0)
		}

		if options.Wait {
			options.Wait = false
			changeWaiter = db.startChangeWaiter(channelsSince.AsSet())
			userCounter = changeWaiter.CurrentUserCount()
			db.initializePrincipalPolling(changeWaiter.GetUserKeys())
			// Reload user to pick up user changes that happened between auth and the change waiter
			// initialization.  Without this, notification for user doc changes in that window (a) won't be
			// included in the initial changes loop iteration, and (b) won't wake up the changeWaiter.
			if db.user != nil {
				if err := db.ReloadUser(); err != nil {
					change := makeErrorEntry("User not found during reload - terminating changes feed")
					output <- &change
					base.Warn("Error reloading user during changes initialization %q: %v", db.user.Name(), err)
					return
				}
			}
		}

		cumulativeClock = base.NewSyncSequenceClock()
		cumulativeClock.SetTo(getChangesClock(options.Since))

		var iterationStartTime time.Time
		// This loop is used to re-run the fetch after every database change, in Wait mode
	outer:
		for {

			if base.TimingExpvarsEnabled {
				iterationStartTime = time.Now()
			}

			// Get the last polled stable sequence.  We don't return anything later than stable sequence in each iteration
			stableClock, err := db.changeCache.GetStableClock(true)
			if err != nil {
				base.Warn("MultiChangesFeed got error reading stable sequence: %v", err)
				change := makeErrorEntry("Error reading stable sequence")
				output <- &change
				return
			}

			// Updates the changeWaiter to the current set of available channels.
			if changeWaiter != nil {
				changeWaiter.UpdateChannels(channelsSince)
			}
			base.LogTo("Changes+", "MultiChangesFeed: channels expand to %#v ... %s", channelsSince.String(), to)

			// postStableSeqsFound tracks whether we hit any sequences later than the stable sequence.  In this scenario the user
			// may not get another wait notification, so we bypass wait loop processing.
			postStableSeqsFound := false

			// Build the channel feeds.
			feeds, postStableSeqsFound, err := db.initializeChannelFeeds(channelsSince, secondaryTriggers, options, addedChannels, userVbNo, cumulativeClock, stableClock)
			if err != nil {
				base.Warn("Error building channel feeds: %v", err)
				change := makeErrorEntry("Error building channel feeds")
				output <- &change
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

				// Don't send backfill notification to clients
				if minEntry.backfill != BackfillFlag_None {
					if minEntry.backfill == BackfillFlag_Complete {
						// Backfill complete means the backfill trigger needs to be added to cumulative clock
						cumulativeClock.SetMaxSequence(minEntry.Seq.vbNo, minEntry.Seq.Seq)
					}
					continue
				}

				if options.ActiveOnly {
					if minEntry.Deleted || minEntry.allRemoved {
						continue
					}
				}

				// Don't send any entries later than the stable sequence
				if stableClock.GetSequence(minEntry.Seq.vbNo) < minEntry.Seq.Seq {
					postStableSeqsFound = true
					dbExpvars.Add("index_changes.postStableSeqs", 1)
					continue
				}

				// Add the doc body or the conflicting rev IDs, if those options are set:
				if options.IncludeDocs || options.Conflicts {
					db.addDocToChangeEntry(minEntry, options)
				}

				// Clock and Hash handling
				// Force new hash generation for non-continuous changes feeds if this is the last entry to be sent - either
				// because there are no more entries in the channel feeds, or we're going to hit the limit.
				forceHash := false
				if options.Continuous == false && (nextEntry == nil || options.Limit == 1) {
					forceHash = true
				}
				// Update the cumulative clock, and stick it on the entry.
				cumulativeClock.SetMaxSequence(minEntry.Seq.vbNo, minEntry.Seq.Seq)

				// Hash when necessary
				if minEntry.Seq.TriggeredByClock == nil {
					lastHashedValue = db.calculateHashWhenNeeded(
						options,
						minEntry,
						cumulativeClock,
						&hashedEntryCount,
						lastHashedValue,
						forceHash,
					)
				} else {
					// All entries triggered by the same sequence reference the same triggered by clock, so it should only need to get hashed once
					// If this is the first entry for this triggered by, initialize the triggered by clock's hash value.
					if minEntry.Seq.TriggeredByClock.GetHashedValue() == "" {
						clockHash, err := db.SequenceHasher.GetHash(cumulativeClock)
						if err != nil {
							base.Warn("Error calculating hash for triggered by clock:%v", base.PrintClock(cumulativeClock))
						} else {
							minEntry.Seq.TriggeredByClock.SetHashedValue(clockHash)
						}
					}
				}

				// Send the entry, and repeat the loop:
				base.LogTo("Changes+", "MultiChangesFeed sending %s %s", minEntry, to)
				select {
				case <-options.Terminator:
					return
				case output <- minEntry:
					if base.TimingExpvarsEnabled {
						base.TimingExpvars.UpdateBySequenceAt("ChangesNotified", minEntry.Seq.vbNo, minEntry.Seq.Seq, iterationStartTime)
						base.TimingExpvars.UpdateBySequence("ChangeEntrySent", minEntry.Seq.vbNo, minEntry.Seq.Seq)
					}
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
				if postStableSeqsFound {
					// If we saw documents later than the stable sequence, use a temporary wait
					// instead of the changeWaiter, as we won't get notified again about those documents
					time.Sleep(kPollFrequency * time.Millisecond)
					break waitForChanges
				}
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
			userChanged, userCounter, addedChannels, err = db.checkForUserUpdatesSince(userCounter, changeWaiter, options.Continuous, channelsSince, options.Since.Clock)
			if userChanged && db.user != nil {
				channelsSince, secondaryTriggers = db.user.FilterToAvailableChannelsForSince(chans, getChangesClock(options.Since))
			}
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

func (db *Database) checkForUserUpdatesSince(userChangeCount uint64, changeWaiter *changeWaiter, isContinuous bool, previousChannels channels.TimedSet, since base.SequenceClock) (isChanged bool, newCount uint64, newChannels base.Set, err error) {

	newCount = changeWaiter.CurrentUserCount()
	// If not continuous, we force user reload as a workaround for https://github.com/couchbase/sync_gateway/issues/2068.  For continuous, #2068 is handled by addedChannels check, and
	// we can reload only when there's been a user change notification
	if newCount > userChangeCount || !isContinuous {
		var newChannels base.Set
		base.LogTo("Changes+", "MultiChangesFeed reloading user %+v", db.user)
		userChangeCount = newCount

		if db.user != nil {
			if err := db.ReloadUser(); err != nil {
				base.Warn("Error reloading user %q: %v", db.user.Name(), err)
				return false, 0, nil, err
			}
			// check whether channels have changed
			newChannels = base.Set{}
			currentChannels, _ := db.user.InheritedChannelsForClock(since)
			for userChannel := range currentChannels {
				_, found := previousChannels[userChannel]
				if !found {
					newChannels[userChannel] = struct{}{}
				}
			}
			if len(newChannels) > 0 {
				base.LogTo("Changes+", "New channels found after user reload: %v", newChannels)
			}
		}
		return true, newCount, newChannels, nil
	}
	return false, userChangeCount, nil, nil
}

func (db *Database) startChangeWaiterSince(chans base.Set, since base.SequenceClock) *changeWaiter {
	waitChans := chans
	if db.user != nil {
		waitChans = db.user.ExpandWildCardChannelSince(chans, since)
	}
	return db.tapListener.NewWaiterWithChannels(waitChans, db.user)
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
	for _, cur := range current {
		if cur != nil && VectorChangesBefore(cur, minEntry) {
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
		if cur != nil && VectorChangesEquals(cur, minEntry) {
			current[i] = nil

			// Skip removed processing if this is a backfill notification
			if cur.backfill != BackfillFlag_None {
				continue
			}
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

// Determine whether two changes entries are equivalent, considering backfill flags along with sequence values
func VectorChangesEquals(c1, c2 *ChangeEntry) bool {
	// Note: BackfillFlag_None and BackfillFlag_Complete are considered equal (no comparison done),
	//     to ensure a backfill complete trigger dedupes with the trigger seq present in another channel
	if c1.backfill == BackfillFlag_Pending || c2.backfill == BackfillFlag_Pending {
		return c1.backfill == c2.backfill
	}
	return c1.Seq.Equals(c2.Seq)
}

// Determine which of two changes entries should be processed first, considering backfill, triggered by and sequence values
func VectorChangesBefore(c1, c2 *ChangeEntry) bool {

	// Empty value handling
	if c1 == nil || c2 == nil {
		if c1 != nil {
			return true
		} else {
			return false
		}
	}

	// We want the following prioritization for changes entries:
	//   1. Backfill in progress
	//      a. If both entries represent entries from an active backfill, compare by vb.seq
	//   2. No backfill in progress
	//       a. Non-backfill and backfill_pending are prioritized over backfill_complete notifications, when vbseq are equal
	//       b. If both entries are non-backfill, compare by vb.seq
	//       c. If both entries are backfill_complete notifications, compare by vb.seq
	s1 := c1.Seq
	s2 := c2.Seq

	// Case 1 checks
	if s1.TriggeredByClock != nil {
		if s2.TriggeredByClock == nil {
			// c1 is backfill, c2 isn't
			return true
		} else {
			// Both backfill, Case 1a.
			return s1.VbucketSequenceBefore(s2.vbNo, s2.Seq)
		}
	}
	if s2.TriggeredByClock != nil {
		// c2 is backfill, c1 isn't
		return false
	}

	// Case 2 checks
	if c1.backfill == BackfillFlag_Complete {
		if c2.backfill == BackfillFlag_Complete {
			// Case 2c. Both backfill complete notifications
			if s1.vbNo == s2.vbNo {
				return s1.Seq < s2.Seq
			} else {
				return s1.vbNo < s2.vbNo
			}
		} else {
			// 2a. if equal, give priority to non-backfill only when vb.seq equal
			if s1.vbNo == s2.vbNo && s1.Seq == s2.Seq {
				return false
			} else {
				return true
			}
		}
	}
	if c2.backfill == BackfillFlag_Complete {
		// 2a - if equal, give priority to non-backfill
		if s1.vbNo == s2.vbNo && s1.Seq == s2.Seq {
			return true
		} else {
			return false
		}
	}

	// Case 2b.
	return s1.VbucketSequenceBefore(s2.vbNo, s2.Seq)

}

// Determines whether the clock hash should be calculated for the entry. For non-continuous changes feeds, hash is only calculated for
// the last entry sent (for use in last_seq), and is done in the defer for the main VectorMultiChangesFeed.
// For continuous changes feeds, we want to calculate the hash for every nth entry, where n=kChangesHashFrequency.  To ensure that
// clients ` restart a new changes feed based on any sequence in the continuous feed, we set the last hash calculated as the LowHash
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
func (db *Database) initializeChannelFeeds(channelsSince channels.TimedSet, secondaryTriggers channels.TimedSet, options ChangesOptions, addedChannels base.Set,
	userVbNo uint16, cumulativeClock *base.SyncSequenceClock, stableClock base.SequenceClock) ([]<-chan *ChangeEntry, bool, error) {
	// Populate the  array of feed channels:
	feeds := make([]<-chan *ChangeEntry, 0, len(channelsSince))
	hasPostChangesTriggers := false

	base.LogTo("Changes+", "GotChannelSince... %v", channelsSince)
	for name, vbSeqAddedAt := range channelsSince {
		seqAddedAt := vbSeqAddedAt.Sequence

		// For user feeds, seqAddedAt == 0 indicates an admin grant hasn't been processed by accel yet (and so
		// hasn't had the granting vbucket sequence stamped on the user doc).
		// Since we don't have a sequence to use for backfill, skip it for now (it will get backfilled
		// after accel updates the user doc with the vb seq)
		if db.user != nil && seqAddedAt == 0 {
			continue
		}

		// If there's no vbNo on the channelsSince, it indicates a user doc channel grant - use the userVbNo.
		var vbAddedAt uint16
		if vbSeqAddedAt.VbNo == nil {
			vbAddedAt = userVbNo
		} else {
			vbAddedAt = *vbSeqAddedAt.VbNo
		}

		base.LogTo("Changes+", "Starting for channel... %s, %d.%d", name, vbAddedAt, seqAddedAt)
		chanOpts := options

		// Check whether requires backfill based on addedChannels in this _changes feed
		isNewChannel := false
		if addedChannels != nil {
			_, isNewChannel = addedChannels[name]
		}

		// Three possible scenarios for backfill handling, based on whether the incoming since value indicates a backfill in progress
		// for this channel, and whether the channel requires a new backfill to be started
		//   Case 1. No backfill in progress, no backfill required - use the incoming since to get changes
		//   Case 2. No backfill in progress, backfill required for this channel.  Start backfill from zero to current since when changes feed reaches trigger vb.seq.
		//   Case 3. Backfill in progress.  Backfill from zero to triggeredByClock, omitting anything earlier than current backfill position.
		backfillInProgress := false
		backfillInOtherChannel := false
		if options.Since.TriggeredByClock != nil {
			// There's a backfill in progress for SOME channel - check if it's this one.  Check:
			//  1. Whether the vb/seq in the TriggeredByClock == vbAddedAt/seqAddedAt-1 for this channel
			if options.Since.TriggeredByClock.GetSequence(vbAddedAt) == seqAddedAt-1 && options.Since.TriggeredByVbNo == vbAddedAt {
				backfillInProgress = true
			} else {
				backfillInOtherChannel = true
			}
		}

		sinceSeq := getChangesClock(options.Since).GetSequence(vbAddedAt)
		backfillRequired := vbSeqAddedAt.Sequence > 0 && sinceSeq < seqAddedAt

		// If backfill required and the triggering seq is after the stable sequence, skip this channel in this iteration
		stableSeq := stableClock.GetSequence(vbAddedAt)
		if backfillRequired && seqAddedAt > stableSeq {
			base.LogTo("Changes+", "Trigger for channel [%s] is post-stable sequence - skipped for this iteration.  userVbNo:[%d] vbAddedAt:[%d] Seq:[%d] Stable seq:[%d]", name, userVbNo, vbAddedAt, seqAddedAt, stableSeq)
			hasPostChangesTriggers = true
			continue
		}

		if isNewChannel || (backfillRequired && !backfillInProgress) {
			// Case 2.  No backfill in progress, backfill required
			chanOpts.Since = SequenceID{
				Seq:             0,
				vbNo:            0,
				TriggeredBy:     seqAddedAt,
				TriggeredByVbNo: vbAddedAt,
			}
			base.LogTo("Changes+", "Starting backfill for channel... %s, %+v", name, chanOpts.Since.Print())
		} else if backfillInProgress {
			// Case 3.  Backfill in progress.
			chanOpts.Since = SequenceID{
				Seq:              options.Since.Seq,
				vbNo:             options.Since.vbNo,
				TriggeredBy:      seqAddedAt,
				TriggeredByVbNo:  vbAddedAt,
				TriggeredByClock: options.Since.TriggeredByClock,
			}
			base.LogTo("Changes+", "Backfill in progress for channel... %s, %+v", name, chanOpts.Since.Print())
		} else if backfillInOtherChannel {
			chanOpts.Since = SequenceID{
				Clock: options.Since.TriggeredByClock, // Update Clock to TriggeredByClock if we're in other backfill
			}
		} else {
			// Case 1.  Leave chanOpts.Since set to options.Since.
		}

		feed, err := db.vectorChangesFeed(name, chanOpts, secondaryTriggers[name], cumulativeClock, stableClock)
		if err != nil {
			base.Warn("MultiChangesFeed got error reading changes feed %q: %v", name, err)
			return feeds, false, err
		}
		feeds = append(feeds, feed)
	}

	// If the user object has changed, create a special pseudo-feed for it:
	if db.user != nil {
		feeds, _ = db.appendVectorUserFeed(feeds, []string{}, options, userVbNo)
	}
	return feeds, hasPostChangesTriggers, nil
}

// Calculates the range for backfill processing, for compatibility with changes streaming.
//  For the given:
//    - since clock SinceClock
//    - current backfill position vb-B.seq-B,
//  We want to return everything for the channel after vb-B.seq-B that's earlier than the since value as backfill
//
// From Clock:
// If vb < vb-B, seq = Since Clock Seq
// If vb = vb-B, seq = seq-b
// if vb > vb-B, seq = 0
// To Clock:
// Since clock

func calculateBackfillRange(backfillPosition base.VbSeq, triggerPosition base.VbSeq, sinceClock base.SequenceClock) (fromClock, toClock base.SequenceClock) {

	fromClock = base.NewSequenceClockImpl()
	toClock = sinceClock

	for vbInt, _ := range sinceClock.Value() {
		vbNo := uint16(vbInt)
		var fromSeq uint64
		// Calculate from sequence for vbucket
		if vbNo < backfillPosition.Vb {
			fromSeq = sinceClock.GetSequence(vbNo)
		} else if vbNo > backfillPosition.Vb {
			fromSeq = 0
		} else if vbNo == backfillPosition.Vb {
			fromSeq = backfillPosition.Seq
		}
		fromClock.SetSequence(vbNo, fromSeq)
	}

	return fromClock, toClock
}

type ChannelFeedType int8

const (
	ChannelFeedType_Standard ChannelFeedType = iota
	ChannelFeedType_ActiveBackfill
	ChannelFeedType_PendingBackfill
)

// Creates a Go-channel of all the changes made on a channel.
// Does NOT handle the Wait option. Does NOT check authorization.
func (db *Database) vectorChangesFeed(channel string, options ChangesOptions, secondaryTrigger channels.VbSequence, cumulativeClock *base.SyncSequenceClock, stableClock base.SequenceClock) (<-chan *ChangeEntry, error) {
	dbExpvars.Add("channelChangesFeeds", 1)
	changeIndex, ok := db.changeCache.(*kvChangeIndex)
	if !ok {
		return nil, fmt.Errorf("Called vectorChangesFeed with non-index cache type: %T", db.changeCache)
	}

	// If we're mid-backfill for this channel, we make one reader call for the backfill and one for non-backfill.  Without
	// two requests, it's not possible to use limit to prevent a full index scan of some vbuckets while still preserving ordering.
	// The second call should only be made if the first request doesn't return limit changes.
	var (
		pendingBackfillLog []*ChangeEntry
		backfillLog        []*LogEntry
		log                []*LogEntry
		err                error
	)

	feedType := ChannelFeedType_Standard
	// Determine whether we're
	if options.Since.TriggeredByClock == nil && options.Since.TriggeredBy > 0 {
		feedType = ChannelFeedType_PendingBackfill
	} else if options.Since.TriggeredByClock != nil {
		feedType = ChannelFeedType_ActiveBackfill
	}

	switch feedType {
	case ChannelFeedType_Standard:
		// Standard (non-backfill) changes feed - get everything from since to stable clock
		log, err = changeIndex.reader.GetChangesForRange(channel, options.Since.Clock, stableClock, options.Limit)
		if err != nil {
			return nil, err
		}
		base.LogTo("Changes+", "[changesFeed] Found %d changes for channel %s", len(log), channel)
	case ChannelFeedType_ActiveBackfill:
		// In-progress backfill for this channel
		// Backfill position: (vb,seq) position in the backfill. e.g. [0,0] if we're just starting the backfill, [vb,seq] if we're midway through.
		backfillPosition := base.VbSeq{options.Since.vbNo, options.Since.Seq}

		// Trigger position: (vb,seq) of the document that triggered this backfill (e.g. access granting doc or user doc)
		triggerPosition := base.VbSeq{options.Since.TriggeredByVbNo, options.Since.TriggeredBy}

		backfillFrom, backfillTo := calculateBackfillRange(backfillPosition, triggerPosition, options.Since.TriggeredByClock)
		backfillLog, err = changeIndex.reader.GetChangesForRange(channel, backfillFrom, backfillTo, options.Limit)
		if err != nil {
			return nil, err
		}
		base.LogTo("Changes+", "[changesFeed] Found %d backfill changes for channel %s", len(backfillLog), channel)
		// If we still have room, get non-backfill entries
		if options.Limit == 0 || len(backfillLog) < options.Limit {
			log, err = changeIndex.reader.GetChangesForRange(channel, backfillTo, nil, options.Limit)
			if err != nil {
				return nil, err
			}
			base.LogTo("Changes+", "[changesFeed] Found %d non-backfill changes for channel %s", len(log), channel)
		}
	case ChannelFeedType_PendingBackfill:
		// Pending backfill for this channel
		pendingBackfillSequence := SequenceID{
			SeqType: ClockSequenceType,
			Seq:     options.Since.TriggeredBy,
			vbNo:    options.Since.TriggeredByVbNo,
		}

		// For pending backfill, we want to block the backfill calculation until the changes feed catches up with the backfill trigger.
		// The main changes loop will read the first entry from the feed channel immediately, and then won't read again until it's time for
		// the backfill.  Since the feed channel is a buffered channel of size 1, we need to write three 'backfill pending' entries to the
		// channel in order to block (first write is read immediately, second write is buffered, third write blocks).
		pendingBackfillLog = make([]*ChangeEntry, 3)

		pendingBackfillLog[0] = &ChangeEntry{
			Seq:      pendingBackfillSequence,
			ID:       fmt.Sprintf("%s0:%s", backfillPendingPrefix, channel),
			backfill: BackfillFlag_Pending,
		}
		pendingBackfillLog[1] = &ChangeEntry{
			Seq:      pendingBackfillSequence,
			ID:       fmt.Sprintf("%s1:%s", backfillPendingPrefix, channel),
			backfill: BackfillFlag_Pending,
		}
		pendingBackfillLog[2] = &ChangeEntry{
			Seq:      pendingBackfillSequence,
			ID:       fmt.Sprintf("%s2:%s", backfillPendingPrefix, channel),
			backfill: BackfillFlag_Pending,
		}

	}

	if len(pendingBackfillLog) == 0 && len(log) == 0 && len(backfillLog) == 0 {
		// There are no entries newer than 'since'. Return an empty feed:
		feed := make(chan *ChangeEntry)
		close(feed)
		return feed, nil
	}

	feed := make(chan *ChangeEntry, 1)
	go func() {
		defer close(feed)

		// Pending backfill processing
		if len(pendingBackfillLog) > 0 {
			for _, changeEntry := range pendingBackfillLog {
				select {
				case <-options.Terminator:
					base.LogTo("Changes+", "Aborting changesFeed")
					return
				case feed <- changeEntry:
				}
			}
			// Retrieve the backfill changes
			// Add the seq before the backfill trigger to the cumulative clock and update triggered by clock for this backfill
			cumulativeClock.SetMaxSequence(options.Since.TriggeredByVbNo, options.Since.TriggeredBy-1)
			// If there's a secondary trigger (occurs when a role grant and role channel grant are post-since), add to the clock
			if secondaryTrigger.Sequence > 0 {
				cumulativeClock.SetMaxSequence(*secondaryTrigger.VbNo, secondaryTrigger.Sequence)
			}

			options.Since.TriggeredByClock = base.NewSequenceClockImpl()
			clockHash, err := db.SequenceHasher.GetHash(cumulativeClock)
			if err != nil {
				base.Warn("Error calculating hash for triggered by clock:%v", base.PrintClock(cumulativeClock))
				return
			}
			options.Since.TriggeredByClock.SetHashedValue(clockHash)

			// Get everything from zero to the cumulative clock as backfill
			backfillLog, err = changeIndex.reader.GetChangesForRange(channel, base.NewSequenceClockImpl(), cumulativeClock, options.Limit)
			if err != nil {
				base.Warn("Error processing backfill changes for channel %s: %v", channel, err)
				return
			}

			// If we still have room, get everything from the cumulative clock to the stable clock as non-backfill
			if options.Limit == 0 || len(backfillLog) < options.Limit {
				log, err = changeIndex.reader.GetChangesForRange(channel, cumulativeClock, stableClock, options.Limit)
				if err != nil {
					base.Warn("Error processing changes for channel %s: %v", channel, err)
					return
				}
				base.LogTo("Changes+", "[changesFeed] Found %d non-backfill changes for channel %s", len(log), channel)
			}
		}

		// Send backfill first
		for _, logEntry := range backfillLog {
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

		if feedType != ChannelFeedType_Standard {
			// If we were in a backfill mode, send a 'backfill complete' entry
			backfillCompleteEntry := ChangeEntry{
				Seq: SequenceID{
					SeqType: ClockSequenceType,
					Seq:     options.Since.TriggeredBy,
					vbNo:    options.Since.TriggeredByVbNo,
				},
				ID:       fmt.Sprintf("%s%s", backfillCompletePrefix, channel),
				backfill: BackfillFlag_Complete,
			}

			select {
			case <-options.Terminator:
				base.LogTo("Changes+", "Aborting changesFeed")
				return
			case feed <- &backfillCompleteEntry:
			}
		}

		// Now send any non-backfill entries.
		for _, logEntry := range log {
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
