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
	"math/rand"
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

// Cache represents a random replacement cache.
type Cache struct {
	size  int                 // Maximum size where this cache can potentially grow upto.
	keys  []string            // A slice of keys for choosing a random key for eviction.
	cache map[string]struct{} // Set of keys for fast lookup.
	lock  sync.RWMutex        // Protects both cache and keys from concurrent access.
}

// Creates a new cache that can potentially grow upto the provided size.
func NewCache(size int) *Cache {
	return &Cache{
		size:  size,
		cache: make(map[string]struct{}, size),
		keys:  make([]string, 0, size),
	}
}

// Contains returns true if the provided key is present
// in the cache and false otherwise.
func (c *Cache) Contains(key string) (ok bool) {
	c.lock.RLock()
	_, ok = c.cache[key]
	c.lock.RUnlock()
	return ok
}

// Put adds a key to the cache. Eviction occurs when memory is
// over filled or greater than the specified size in the cache.
// Keys are evicted randomly from the cache.
func (c *Cache) Put(key string) {
	c.lock.Lock()
	if _, ok := c.cache[key]; ok {
		c.lock.Unlock()
		return
	}
	if len(c.cache) >= c.size {
		index := rand.Intn(len(c.keys))
		delete(c.cache, c.keys[index])
		c.keys[index] = key
	} else {
		c.keys = append(c.keys, key)
	}
	c.cache[key] = struct{}{}
	c.lock.Unlock()
}

// Len returns the number of keys in the cache.
func (c *Cache) Len() int {
	c.lock.RLock()
	length := len(c.cache)
	c.lock.RUnlock()
	return length
}

// CompareHashAndPassword is an optimized wrapper around bcrypt.CompareHashAndPassword that
// caches successful results in memory to avoid the _very_ high overhead of calling bcrypt.
func (c *Cache) CompareHashAndPassword(hash []byte, password []byte) bool {
	// Actually we cache the SHA1 digest of the password to avoid keeping passwords in RAM.
	key := authKey(hash, password)
	if c.Contains(key) {
		return true
	}
	// Cache missed; now we make the very slow (~100ms) bcrypt call:
	if err := bcrypt.CompareHashAndPassword(hash, password); err != nil {
		// Note: It's important to only cache successful matches, not failures.
		// Failure is supposed to be slow, to make online attacks impractical.
		return false
	}
	c.Put(key)
	return true
}
