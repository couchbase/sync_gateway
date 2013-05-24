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
)

// A mutable mapping from channel names to sequence numbers (interpreted as the sequence when
// the channel was added.)
type TimedSet map[string]uint64

// Creates a new TimedSet from a Set plus a sequence
func (set Set) AtSequence(sequence uint64) TimedSet {
	result := make(TimedSet, len(set))
	for name, _ := range set {
		result[name] = sequence
	}
	return result
}

// Converts a TimedSet to a Set
func (set TimedSet) AsSet() Set {
	if set == nil {
		return nil
	}
	result := make(Set, len(set))
	for ch, _ := range set {
		result[ch] = present{}
	}
	return result
}

func (set TimedSet) Validate() error {
	for name, _ := range set {
		if !IsValidChannel(name) {
			return fmt.Errorf("Illegal channel name %q", name)
		}
	}
	return nil
}

func (set TimedSet) AllChannels() []string {
	result := make([]string, 0, len(set))
	for name, _ := range set {
		result = append(result, name)
	}
	return result
}

func (set TimedSet) Copy() TimedSet {
	result := make(TimedSet, len(set))
	for name, sequence := range set {
		result[name] = sequence
	}
	return result
}

// Returns true if the set includes the channel.
func (set TimedSet) Contains(ch string) bool {
	_, exists := set[ch]
	return exists
}

// Updates membership to match the given Set. Newly added members will have the given sequence.
func (set TimedSet) UpdateAtSequence(other Set, sequence uint64) bool {
	changed := false
	for name, _ := range set {
		if !other.Contains(name) {
			delete(set, name)
			changed = true
		}
	}
	for name, _ := range other {
		if !set.Contains(name) {
			set[name] = sequence
			changed = true
		}
	}
	return changed
}

func (set TimedSet) AddChannel(channelName string, atSequence uint64) bool {
	if atSequence > 0 {
		if oldSequence := set[channelName]; oldSequence == 0 || atSequence < oldSequence {
			set[channelName] = atSequence
			return true
		}
	}
	return false
}

// Merges the other set into the receiver. In case of collisions the earliest sequence wins.
func (set TimedSet) Add(other TimedSet) bool {
	changed := false
	for ch, sequence := range other {
		if set.AddChannel(ch, sequence) {
			changed = true
		}
	}
	return changed
}

// TimedSet can unmarshal either from the regular format {"channel":sequence, ...}
// or from an array of channel names. In the latter case all the sequences will be 0.
func (setPtr *TimedSet) UnmarshalJSON(data []byte) error {
	var normalForm map[string]uint64
	if err := json.Unmarshal(data, &normalForm); err != nil {
		var altForm []string
		if err2 := json.Unmarshal(data, &altForm); err2 == nil {
			set, err := SetFromArray(altForm, KeepStar)
			if err == nil {
				*setPtr = set.AtSequence(0)
			}
			return err
		}
		return err
	}
	*setPtr = TimedSet(normalForm)
	return nil

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
	for channel, seqNo := range set {
		if seqNo > 0 {
			items = append(items, fmt.Sprintf("%s:%d", channel, seqNo))
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
			seqNo, err := strconv.ParseUint(components[1], 10, 64)
			if err != nil || seqNo == 0 || !IsValidChannel(channel) {
				return nil
			}
			if _, found := set[channel]; found {
				return nil // duplicate channel
			}
			set[channel] = seqNo
		}
	}
	return set
}
