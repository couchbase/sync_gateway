//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package channels

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

// Vb and Sequence struct that's compatible with sequence-only (global sequence) mode
type VbSequence struct {
	VbNo     *uint16 `json:"vb,omitempty"`
	Sequence uint64  `json:"seq"`
}

func NewVbSequence(vbNo uint16, sequence uint64) VbSequence {
	return VbSequence{VbNo: &vbNo, Sequence: sequence}
}

func NewVbSimpleSequence(sequence uint64) VbSequence {
	return VbSequence{Sequence: sequence}
}

func (vbs VbSequence) String() string {
	if vbs.VbNo != nil {
		return fmt.Sprintf("[%d:%d]", *vbs.VbNo, vbs.Sequence)
	} else {
		return fmt.Sprintf("[nil:%d]", vbs.Sequence)
	}
}

func (vbs VbSequence) Copy() VbSequence {
	if vbs.VbNo == nil {
		return NewVbSimpleSequence(vbs.Sequence)
	} else {
		vbInt := *vbs.VbNo
		return NewVbSequence(vbInt, vbs.Sequence)
	}
}

func (vbs VbSequence) Equals(other VbSequence) bool {
	if vbs.Sequence != other.Sequence {
		return false
	}

	if vbs.VbNo == nil {
		if other.VbNo != nil {
			return false
		}
		return true
	} else {
		if other.VbNo == nil {
			return false
		}
		return *vbs.VbNo == *other.VbNo
	}
}

func (vbs VbSequence) AsVbSeq() base.VbSeq {
	if vbs.VbNo == nil {
		return base.VbSeq{}
	}
	return base.VbSeq{*vbs.VbNo, vbs.Sequence}
}

// Compares to other VbSequence.  If EITHER vbNo is nil, does a sequence-only
// comparison
func (v VbSequence) CompareTo(other VbSequence) base.CompareResult {
	if v.VbNo == nil || other.VbNo == nil {
		return base.CompareVbAndSequence(0, v.Sequence, 0, other.Sequence)
	}
	return base.CompareVbAndSequence(*v.VbNo, v.Sequence, *other.VbNo, other.Sequence)
}

// Is sequence less than or equal to corresponding clock entry
func (v VbSequence) IsLTEClock(clock base.SequenceClock) bool {
	if v.VbNo == nil {
		return false
	}
	return v.Sequence <= clock.GetSequence(*v.VbNo)
}

// A mutable mapping from channel names to sequence numbers (interpreted as the sequence when
// the channel was added.)
type TimedSet map[string]VbSequence

// Creates a new TimedSet from a Set plus a sequence
func AtSequence(set base.Set, sequence uint64) TimedSet {
	result := make(TimedSet, len(set))
	for name := range set {
		result[name] = NewVbSimpleSequence(sequence)
	}
	return result
}

// Converts a TimedSet to a Set
func (set TimedSet) AsSet() base.Set {
	if set == nil {
		return nil
	}
	result := make([]string, 0, len(set))
	for ch := range set {
		result = append(result, ch)
	}
	return base.SetFromArray(result)
}

func (set TimedSet) Validate() error {
	for name := range set {
		if !IsValidChannel(name) {
			return illegalChannelError(name)
		}
	}
	return nil
}

func (set TimedSet) AllChannels() []string {
	result := make([]string, 0, len(set))
	for name := range set {
		result = append(result, name)
	}
	return result
}

func (set TimedSet) Copy() TimedSet {
	result := make(TimedSet, len(set))
	for name, sequence := range set {
		result[name] = sequence.Copy()
	}
	return result
}

// Returns true if the set includes the channel.
func (set TimedSet) Contains(ch string) bool {
	_, exists := set[ch]
	return exists
}

// Updates membership to match the given Set. Newly added members will have the given sequence.
func (set TimedSet) UpdateAtSequence(other base.Set, sequence uint64) bool {
	changed := false
	for name := range set {
		if !other.Contains(name) {
			delete(set, name)
			changed = true
		}
	}
	for name := range other {
		if !set.Contains(name) {
			set[name] = NewVbSimpleSequence(sequence)
			changed = true
		}
	}
	return changed
}

// Check for matching entry names, ignoring sequence
func (set TimedSet) Equals(other base.Set) bool {

	for name := range set {
		if !other.Contains(name) {
			return false
		}
	}
	for name := range other {
		if !set.Contains(name) {
			return false
		}
	}
	return true
}

func (set TimedSet) AddChannel(channelName string, atSequence uint64) bool {
	if atSequence > 0 {
		if oldSequence := set[channelName]; oldSequence.Sequence == 0 || atSequence < oldSequence.Sequence {
			set[channelName] = NewVbSimpleSequence(atSequence)
			return true
		}
	}
	return false
}

// Merges the other set into the receiver. In case of collisions the earliest sequence wins.
func (set TimedSet) Add(other TimedSet) bool {
	return set.AddAtSequence(other, 0)
}

// Merges the other set into the receiver at a given sequence. */
func (set TimedSet) AddAtSequence(other TimedSet, atSequence uint64) bool {
	changed := false
	for ch, vbSeq := range other {
		// If vbucket is present, do a straight replace
		if vbSeq.VbNo != nil {
			set[ch] = vbSeq
			changed = true
		} else {
			if vbSeq.Sequence < atSequence {
				vbSeq.Sequence = atSequence
			}
			if set.AddChannel(ch, vbSeq.Sequence) {
				changed = true
			}
		}
	}
	return changed
}

// Merges the other set into the receiver at a given sequence. */
func (set TimedSet) AddAtVbSequence(other TimedSet, atVbSequence VbSequence) bool {
	changed := false
	for ch, _ := range other {
		set[ch] = atVbSequence
		changed = true
	}
	return changed
}

// For any channel present in both the set and the other set, updates the sequence to the value
// from the other set
func (set TimedSet) UpdateIfPresent(other TimedSet) {
	for ch := range set {
		if otherSeq, ok := other[ch]; ok {
			set[ch] = otherSeq
		}
	}
}

// TimedSet can unmarshal from either:
//   1. The regular format {"channel":vbSequence, ...}
//   2. The sequence-only format {"channel":uint64, ...} or
//   3. An array of channel names.
// In the last two cases, all vbNos will be 0.
// In the latter case all the sequences will be 0.
func (setPtr *TimedSet) UnmarshalJSON(data []byte) error {

	var normalForm map[string]VbSequence
	if err := json.Unmarshal(data, &normalForm); err != nil {
		var sequenceOnlyForm map[string]uint64
		if err := json.Unmarshal(data, &sequenceOnlyForm); err != nil {
			var altForm []string
			if err2 := json.Unmarshal(data, &altForm); err2 == nil {
				set, err := SetFromArray(altForm, KeepStar)
				if err == nil {
					*setPtr = AtSequence(set, 0)
				}
				return err
			}
			return err
		}
		*setPtr = TimedSetFromSequenceOnlySet(sequenceOnlyForm)
		return nil
	}
	*setPtr = TimedSet(normalForm)
	return nil
}

func (set TimedSet) MarshalJSON() ([]byte, error) {

	// If no vbuckets are defined, marshal as SequenceOnlySet for backwards compatibility.  Otherwise marshal with vbuckets
	hasVbucket := false
	for _, vbSeq := range set {
		if vbSeq.VbNo != nil {
			hasVbucket = true
			break
		}
	}
	if hasVbucket {
		// Normal form - unmarshal as map[string]VbSequence.  Need to convert back to simple map[string]VbSequence to avoid
		// having json.Marshal just call back into this function.
		// Marshals entries as "ABC":{"vb":5,"seq":1} or "CBS":{"seq":1}, depending on whether VbSequence.VbNo is nil
		var plainMap map[string]VbSequence
		plainMap = set
		return json.Marshal(plainMap)
	} else {
		// Sequence-only form.
		// Marshals entries as "ABC":1, "CBS":1  - backwards compatibility when vbuckets aren't present.
		return json.Marshal(set.SequenceOnlySet())
	}
}

func (set TimedSet) SequenceOnlySet() map[string]uint64 {
	var sequenceOnlySet map[string]uint64
	if set != nil {
		sequenceOnlySet = make(map[string]uint64)
		for ch, vbSeq := range set {
			sequenceOnlySet[ch] = vbSeq.Sequence
		}
	}
	return sequenceOnlySet
}

func TimedSetFromSequenceOnlySet(sequenceOnlySet map[string]uint64) TimedSet {
	var timedSet TimedSet
	if sequenceOnlySet != nil {
		timedSet = make(TimedSet)
		for ch, sequence := range sequenceOnlySet {
			timedSet[ch] = NewVbSimpleSequence(sequence)
		}
	}
	return timedSet
}

//////// STRING ENCODING:

// This is a simple compact round-trippable string encoding. It's used for sequence IDs in the
// public REST API.
// Note: Making incompatible changes to the format of these strings will potentially invalidate
// the saved checkpoint of every pull replication of every client in the world. This isn't fatal
// but will cause those replications to start over from the beginning. Think first.

// Encodes a TimedSet as a string (as sent in the public _changes feed.)
// This string can later be turned back into a TimedSet by calling TimedSetFromString().
func (set TimedSet) String() string {
	var items []string
	for channel, vbSeq := range set {
		if vbSeq.Sequence > 0 {
			if vbSeq.VbNo != nil {
				items = append(items, fmt.Sprintf("%s:%d.%d", channel, *vbSeq.VbNo, vbSeq.Sequence))
			} else {
				items = append(items, fmt.Sprintf("%s:%d", channel, vbSeq.Sequence))
			}
		}
	}
	sort.Strings(items) // not strictly necessary but makes the string reproducible
	return strings.Join(items, ",")
}

// Parses a string as generated from TimedSet.String().
// Returns nil on failure. An empty string successfully parses to an empty TimedSet.
func TimedSetFromString(encoded string) TimedSet {
	items := strings.Split(encoded, ",")
	set := make(TimedSet, len(items))
	if encoded != "" {
		for _, item := range items {
			components := strings.Split(item, ":")
			if len(components) != 2 {
				return nil
			}
			channel := components[0]
			if _, found := set[channel]; found {
				return nil // duplicate channel
			}
			if !IsValidChannel(channel) {
				return nil
			}
			// VB sequence handling
			if strings.Contains(components[1], ".") {
				seqComponents := strings.Split(components[1], ".")
				if len(seqComponents) != 2 {
					return nil
				}
				vbNo, err := strconv.ParseUint(seqComponents[0], 10, 16)
				vbSeq, err := strconv.ParseUint(seqComponents[1], 10, 64)
				if err != nil {
					return nil
				}
				set[channel] = NewVbSequence(uint16(vbNo), vbSeq)
			} else {
				// Simple sequence handling
				seqNo, err := strconv.ParseUint(components[1], 10, 64)
				if err != nil || seqNo == 0 {
					return nil
				}
				set[channel] = NewVbSimpleSequence(seqNo)
			}
		}
	}
	return set
}
