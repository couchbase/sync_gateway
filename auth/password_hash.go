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
	"crypto/sha1"
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"

	"github.com/couchbase/sync_gateway/base"
)

var (
	// bcryptDefaultCost is the default bcrypt cost to use
	bcryptDefaultCost = bcrypt.DefaultCost
	// bcryptCost is used when hashing new passwords
	// either via password reset, or rehashPassword
	bcryptCost = bcryptDefaultCost
	// Used to exit-early for unchanged values
	bcryptCostChanged = false
)

var ErrInvalidBcryptCost = fmt.Errorf("invalid bcrypt cost")

// The maximum number of pairs to keep in the below auth cache.
const kMaxCacheSize = 25000

// Set of known-to-be-valid {password, bcryt-hash} pairs.
// Keys are of the form SHA1 digest of password + bcrypt'ed hash of password
var cachedHashes = NewCache(kMaxCacheSize)

// authKey returns the bcrypt hash + SHA1 digest of the password.
func authKey(hash []byte, password []byte) (key string) {
	s := sha1.New()
	s.Write(password)
	digest := string(s.Sum(nil))
	key = digest + string(hash)
	return key
}

// Optimized wrapper around bcrypt.CompareHashAndPassword that caches successful results in
// memory to avoid the _very_ high overhead of calling bcrypt.
func compareHashAndPassword(hash []byte, password []byte) bool {

	// Actually we cache the SHA1 digest of the password to avoid keeping passwords in RAM.
	key := authKey(hash, password)
	if cachedHashes.Contains(key) {
		return true
	}

	// Cache missed; now we make the very slow (~100ms) bcrypt call:
	if err := bcrypt.CompareHashAndPassword(hash, password); err != nil {
		// Note: It's important to only cache successful matches, not failures.
		// Failure is supposed to be slow, to make online attacks impractical.
		return false
	}

	cachedHashes.Put(key)
	return true
}

// SetBcryptCost will set the bcrypt cost for Sync Gateway to use
// Values of zero or less will use bcryptDefaultCost instead
// An error is returned if the cost is not between bcryptDefaultCost and bcrypt.MaxCost
func SetBcryptCost(cost int) error {
	if cost <= 0 {
		bcryptCost = bcryptDefaultCost
		base.Debugf(base.KeyAuth, "bcrypt cost set to default: %d", cost)
		return nil
	}

	if cost < bcryptDefaultCost || cost > bcrypt.MaxCost {
		return errors.Wrapf(ErrInvalidBcryptCost,
			"%d outside allowed range: %d-%d",
			cost, bcryptDefaultCost, bcrypt.MaxCost)
	}

	base.Infof(base.KeyAuth, "bcrypt cost set to: %d", cost)
	bcryptCost = cost
	bcryptCostChanged = true

	return nil
}
