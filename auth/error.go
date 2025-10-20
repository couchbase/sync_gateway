// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package auth

import (
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
)

var (
	errLoginRequired = base.HTTPErrorf(http.StatusUnauthorized, "login required")
	// errUnauthorized is a generic error message
	errUnauthorized = base.HTTPErrorf(http.StatusForbidden, "You are not allowed to see this")
	// errUnauthorizedChannels is used when we cannot determine which channels are unauthorized
	errUnauthorizedChannels = base.HTTPErrorf(http.StatusForbidden, "Unauthorized to see channels")
	// errNotAllowedChannels is used when we can determine which channels are not allowed
	errNotAllowedChannels = base.HTTPErrorf(http.StatusForbidden, "You are not allowed to see channels")
)

// newErrUnauthorizedChannels creates an error indicating the user is not authorized to see the specified channels. Used when we can not determine which channels are unauthorized.
func newErrUnauthorizedChannels(channels base.Set) error {
	return fmt.Errorf("%w %v", errUnauthorizedChannels, channels)
}

// newErrNotAllowedChannels creates an error indicating the user is not allowed to see the specified channels. Used when we can determine which channels are not allowed.
func newErrNotAllowedChannels[T base.Set | []string](channels T) error {
	return fmt.Errorf("%w %v", errNotAllowedChannels, channels)
}
