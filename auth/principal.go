package auth

import (
	ch "github.com/couchbaselabs/sync_gateway/channels"
)

// A Principal is an abstract object that can have access to channels.
type Principal interface {
	Name() string
	Channels() ch.Set
	ExplicitChannels() ch.Set

	CanSeeChannel(channel string) bool
	AuthorizeAllChannels(channels ch.Set) error
	UnauthError(message string) error

	docID() string
	accessViewKey() string
	validate() error
	setChannels(ch.Set)
}

// Role is basically the same as Principal, just concrete. Users can inherit channels from Roles.
type Role interface {
	Principal
}

// A User is a Principal that can log in and have multiple Roles.
type User interface {
	Principal

	Email() string
	SetEmail(string) error
	Disabled() bool
	Authenticate(password string) bool
	SetPassword(password string)

	RoleNames() []string
	SetRoleNames([]string)

	InheritedChannels() ch.Set
	ExpandWildCardChannel(channels ch.Set) ch.Set
}
