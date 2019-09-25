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
	"sync"

	"github.com/couchbase/sync_gateway/base"
	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
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

// Set of known-to-be-valid {password, bcryt-hash} pairs.
// Keys are of the form SHA1 digest of password + bcrypt'ed hash of password
var cachedHashes = map[string]struct{}{}
var cacheLock sync.RWMutex

// The maximum number of pairs to keep in the above cache
const kMaxCacheSize = 25000

// Optimized wrapper around bcrypt.CompareHashAndPassword that caches successful results in
// memory to avoid the _very_ high overhead of calling bcrypt.
func compareHashAndPassword(hash []byte, password []byte) bool {
	// Actually we cache the SHA1 digest of the password to avoid keeping passwords in RAM.
	s := sha1.New()
	s.Write(password)
	digest := string(s.Sum(nil))
	key := digest + string(hash)

	cacheLock.RLock()
	_, valid := cachedHashes[key]
	cacheLock.RUnlock()
	if valid {
		return true
	}

	// Cache missed; now we make the very slow (~100ms) bcrypt call:
	if err := bcrypt.CompareHashAndPassword(hash, password); err != nil {
		// Note: It's important to only cache successful matches, not failures.
		// Failure is supposed to be slow, to make online attacks impractical.
		return false
	}

	cacheLock.Lock()
	// TODO: Replace this with an LRU cache so the whole map doesn't get wiped
	if len(cachedHashes) >= kMaxCacheSize {
		cachedHashes = map[string]struct{}{}
	}
	cachedHashes[key] = struct{}{}
	cacheLock.Unlock()

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
