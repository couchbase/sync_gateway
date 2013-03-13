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
	CanSeeAllChannels(channels ch.Set) bool
	AuthorizeAllChannels(channels ch.Set) error
	ExpandWildCardChannel(channels ch.Set) ch.Set

	docID() string
	accessViewKey() string
	validate() error
	setChannels(ch.Set)
}

type User interface {
	Principal

	Email() string
	SetEmail(string) error
	Disabled() bool
	Authenticate(password string) bool
	UnauthError(message string) error
	SetPassword(password string)
	RoleNames() []string
}

type Role interface {
	Principal
}
