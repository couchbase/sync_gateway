//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package channels

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

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

// ID represents a single channel inside a collection
type ID struct {
	Name          string // name of channel
	CollectionID  uint32 // collection it belongs to
	serialization string // private method for logging and matching inside changeWaiter notification
}

func (c ID) String() string {
	return c.serialization
}

// NewID returns a new ChannelID
func NewID(channelName string, collectionID uint32) ID {
	return ID{
		Name:          channelName,
		CollectionID:  collectionID,
		serialization: strconv.FormatUint(uint64(collectionID), 10) + "." + channelName,
	}
}

type Set map[ID]present

type present struct{}

func illegalChannelError(name string) error {
	return base.HTTPErrorf(400, "Illegal channel name %q", name)
}

func IsValidChannel(channel string) bool {
	return len(channel) > 0 && !strings.Contains(channel, ",")
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

// SetOf creates a new Set. Returns an error if any names are invalid.
func SetOf(chans ...ID) (Set, error) {
	result := make(Set, len(chans))
	for _, ch := range chans {
		if !IsValidChannel(ch.Name) {
			return nil, illegalChannelError(ch.Name)
		}
		result[ch] = present{}
	}
	return result, nil
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

// SetFromArrayNoValidate creates a set of channels without validating they are valid names.
func SetFromArrayNoValidate(chans []ID) Set {
	result := make(Set, len(chans))
	for _, ch := range chans {
		result[ch] = present{}
	}
	return result
}

// SetOfNoValidate creates a new Set from inline channels.
func SetOfNoValidate(chans ...ID) Set {
	return SetFromArrayNoValidate(chans)
}

// Update adds all elements from other set and returns the union of the sets.
func (s Set) Update(other Set) Set {
	if len(s) == 0 {
		return other
	} else if len(other) == 0 {
		return s
	}
	for ch := range other {
		s[ch] = present{}
	}
	return s
}

// Add a channel to a set.
func (s Set) Add(value ID) Set {
	s[value] = present{}
	return s
}

// UpdateWithSlice adds channels to a Set.
func (s Set) UpdateWithSlice(slice []ID) Set {
	if len(slice) == 0 {
		return s
	} else if len(s) == 0 {
		s = make(Set, len(slice))
	}
	for _, ch := range slice {
		s[ch] = present{}
	}
	return s
}

// Contains returns true if the set includes the channel.
func (s Set) Contains(ch ID) bool {
	_, exists := s[ch]
	return exists
}

// Convert to String(), necessary for logging.
func (s Set) String() string {
	keys := make([]string, len(s))
	for ch := range s {
		keys = append(keys, ch.String())
	}
	sort.Strings(keys)
	return fmt.Sprintf("{%s}", strings.Join(keys, ", "))
}
