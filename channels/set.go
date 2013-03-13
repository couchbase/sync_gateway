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
	"regexp"
	"sort"
	"strings"
)

// An immutable set of channel names, represented as a map.
type Set map[string]present

type present struct{}

type StarMode int

const (
	RemoveStar = StarMode(iota)
	KeepStar
	ExpandStar
)

var kValidChannelRegexp *regexp.Regexp

func init() {
	var err error
	kValidChannelRegexp, err = regexp.Compile(`^([-+=_.@\p{L}\p{Nd}]+|\*)$`)
	if err != nil {
		panic("Bad IsValidChannel regexp")
	}
}

func IsValidChannel(channel string) bool {
	return kValidChannelRegexp.MatchString(channel)
}

// Creates a new Set from an array of strings. Returns an error if any names are invalid.
func SetFromArray(names []string, mode StarMode) (Set, error) {
	result := make(Set, len(names))
	for _, name := range names {
		if !IsValidChannel(name) {
			return nil, fmt.Errorf("Illegal channel name %q", name)
		} else if name == "*" {
			if mode == RemoveStar {
				continue
			} else if mode == ExpandStar {
				return Set{"*": present{}}, nil
			}
		}
		result[name] = present{}
	}
	return result, nil
}

// Creates a set from zero or more inline string arguments.
// Channel names must be valid, else the function will panic, so this should only be called
// with hardcoded known-valid strings.
func SetOf(names ...string) Set {
	set, err := SetFromArray(names, KeepStar)
	if err != nil {
		panic(fmt.Sprintf("SetOf failed: %v", err))
	}
	return set
}

// Converts a Set to an array of strings (ordering is undefined).
func (set Set) ToArray() []string {
	result := make([]string, 0, len(set))
	for name, _ := range set {
		result = append(result, name)
	}
	return result
}

func (set Set) String() string {
	list := set.ToArray()
	sort.Strings(list)
	return fmt.Sprintf("{%s}", strings.Join(list, ", "))
}

func (set Set) copy() Set {
	result := make(Set, len(set))
	for name, _ := range set {
		result[name] = present{}
	}
	return result
}

// Returns true if the set includes the channel.
func (set Set) Contains(ch string) bool {
	_, exists := set[ch]
	return exists
}

// Returns the union of two sets.
func (set Set) Union(other Set) Set {
	if len(set) == 0 {
		return other
	} else if len(other) == 0 {
		return set
	}
	result := set.copy()
	for ch, _ := range other {
		result[ch] = present{}
	}
	return result
}

// If the set contains "*", returns a set of only "*". Else returns the original set.
func (set Set) ExpandingStar() Set {
	if _, exists := set["*"]; exists {
		return Set{"*": present{}}
	}
	return set
}

// Returns a set with any "*" channel removed.
func (set Set) IgnoringStar() Set {
	if _, exists := set["*"]; exists {
		set = set.copy()
		delete(set, "*")
	}
	return set
}

// JSON encoding/decoding:

func (set Set) MarshalJSON() ([]byte, error) {
	list := set.ToArray()
	sort.Strings(list) // sort the array so it's written in a consistent order; helps testability
	return json.Marshal(list)
}

func (setPtr *Set) UnmarshalJSON(data []byte) error {
	if *setPtr == nil {
		*setPtr = Set{}
	}
	set := *setPtr
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return err
	}
	if len(set) > 0 {
		panic("unmarshaling into non-empty Set") // I don't _think_ this can ever occur?
	}
	for _, name := range names {
		set[name] = present{}
	}
	return nil
}
