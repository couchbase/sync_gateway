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
	"fmt"
	"regexp"

	"github.com/couchbase/sync_gateway/base"
)

type StarMode int

const (
	RemoveStar = StarMode(iota)
	KeepStar
	ExpandStar
)

// Constants for the * channel variations
const UserStarChannel = "*"     // user channel for "can access all docs"
const DocumentStarChannel = "!" // doc channel for "visible to all users"
const AllChannelWildcard = "*"  // wildcard for 'all channels'

var kValidChannelRegexp *regexp.Regexp

func init() {
	var err error
	kValidChannelRegexp, err = regexp.Compile(`,`)
	if err != nil {
		panic("Bad IsValidChannel regexp")
	}
}

func illegalChannelError(name string) error {
	return base.HTTPErrorf(400, "Illegal channel name %q", name)
}

func IsValidChannel(channel string) bool {
	return len(channel) > 0 && !kValidChannelRegexp.MatchString(channel)
}

// Creates a new Set from an array of strings. Returns an error if any names are invalid.
func SetFromArray(names []string, mode StarMode) (base.Set, error) {
	for _, name := range names {
		if !IsValidChannel(name) {
			return nil, illegalChannelError(name)
		}
	}
	result := base.SetFromArray(names)
	switch mode {
	case RemoveStar:
		result = result.Removing(UserStarChannel)
	case ExpandStar:
		if result.Contains(UserStarChannel) {
			result = base.SetOf(UserStarChannel)
		}
	}
	return result, nil
}

func ValidateChannelSet(set base.Set) error {
	for name := range set {
		if !IsValidChannel(name) {
			return illegalChannelError(name)
		}
	}
	return nil
}

// Creates a set from zero or more inline string arguments.
// Channel names must be valid, else the function will panic, so this should only be called
// with hardcoded known-valid strings.
func SetOf(names ...string) base.Set {
	set, err := SetFromArray(names, KeepStar)
	if err != nil {
		panic(fmt.Sprintf("channels.SetOf failed: %v", err))
	}
	return set
}

// If the set contains "*", returns a set of only "*". Else returns the original set.
func ExpandingStar(set base.Set) base.Set {
	if _, exists := set[UserStarChannel]; exists {
		return base.SetOf(UserStarChannel)
	}
	return set
}

// Returns a set with any "*" channel removed.
func IgnoringStar(set base.Set) base.Set {
	return set.Removing(UserStarChannel)
}
