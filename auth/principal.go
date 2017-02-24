//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package auth

import (
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

// A Principal is an abstract object that can have access to channels.
type Principal interface {
	// The Principal's identifier.
	Name() string

	// The database sequence at which this Principal last changed
	Sequence() uint64
	SetSequence(sequence uint64)

	// The set of channels the Principal belongs to, and what sequence access was granted.
	Channels() ch.TimedSet

	// The channels the Principal was explicitly granted access to thru the admin API.
	ExplicitChannels() ch.TimedSet

	// Sets the explicit channels the Principal has access to.
	SetExplicitChannels(ch.TimedSet)

	// The previous set of channels the Principal was granted.  Used to maintain sequence history.
	PreviousChannels() ch.TimedSet

	// Sets the previous set of channels the Principal has access to.
	SetPreviousChannels(ch.TimedSet)

	// Returns true if the Principal has access to the given channel.
	CanSeeChannel(channel string) bool

	// If the Principal has access to the given channel, returns the sequence number at which
	// access was granted; else returns zero.
	CanSeeChannelSince(channel string) uint64

	// If the Principal has access to the given channel, returns the vb and sequence number at which
	// access was granted; else returns zero.
	CanSeeChannelSinceVbSeq(channel string, hashFunction VBHashFunction) (base.VbSeq, bool)

	// Validate that the specified vbSeq has a non-zero sequence, and populate the vbucket for
	// admin grants.
	ValidateGrant(vbseq *ch.VbSequence, hashFunction VBHashFunction) bool

	// Returns an error if the Principal does not have access to all the channels in the set.
	AuthorizeAllChannels(channels base.Set) error

	// Returns an error if the Principal does not have access to any of the channels in the set.
	AuthorizeAnyChannel(channels base.Set) error

	// Returns an appropriate HTTPError for unauthorized access -- a 401 if the receiver is
	// the guest user, else 403.
	UnauthError(message string) error

	DocID() string
	accessViewKey() string
	validate() error
	setChannels(ch.TimedSet)
	getVbNo(hashFunction VBHashFunction) uint16
}

// Role is basically the same as Principal, just concrete. Users can inherit channels from Roles.
type Role interface {
	Principal
}

// A User is a Principal that can log in and have multiple Roles.
type User interface {
	Principal

	// The user's email address.
	Email() string

	// Sets the user's email address.
	SetEmail(string) error

	// If true, the user is unable to authenticate.
	Disabled() bool

	// Sets the disabled property
	SetDisabled(bool)

	// Authenticates the user's password.
	Authenticate(password string) bool

	// Changes the user's password.
	SetPassword(password string)

	// The set of Roles the user belongs to (including ones given to it by the sync function)
	RoleNames() ch.TimedSet

	// The roles the user was explicitly granted access to thru the admin API.
	ExplicitRoles() ch.TimedSet

	// Sets the explicit roles the user belongs to.
	SetExplicitRoles(ch.TimedSet)

	// Every channel the user has access to, including those inherited from Roles.
	InheritedChannels() ch.TimedSet

	// If the input set contains the wildcard "*" channel, returns the user's InheritedChannels;
	// else returns the input channel list unaltered.
	ExpandWildCardChannel(channels base.Set) base.Set

	// Returns a TimedSet containing only the channels from the input set that the user has access
	// to, annotated with the sequence number at which access was granted.
	FilterToAvailableChannels(channels base.Set) ch.TimedSet

	// Every channel the user has access to, including those inherited from Roles.
	InheritedChannelsForClock(since base.SequenceClock) (channels ch.TimedSet, secondaryTriggers ch.TimedSet)

	// If the input set contains the wildcard "*" channel, returns the user's InheritedChannels, restricted
	// by the since value;
	// else returns the input channel list unaltered.
	ExpandWildCardChannelSince(channels base.Set, since base.SequenceClock) base.Set

	// Returns a TimedSet containing only the channels from the input set that the user has access
	// to, annotated with the sequence number at which access was granted.  When there are multiple grants
	// to the same channel, priority is given to values prior to the specified since.
	FilterToAvailableChannelsForSince(channels base.Set, since base.SequenceClock) (ch.TimedSet, ch.TimedSet)

	// Returns a Set containing channels that the user has access to, that aren't present in the
	// input set
	GetAddedChannels(channels ch.TimedSet) base.Set

	setRolesSince(ch.TimedSet)
}
