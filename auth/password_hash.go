//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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

var ErrInvalidBcryptCost = fmt.Errorf("invalid bcrypt cost")

// The maximum number of pairs to keep in the below auth cache.
const kMaxCacheSize = 25000

// Set of known-to-be-valid {password, bcryt-hash} pairs.
// Keys are of the form SHA1 digest of password + bcrypt'ed hash of password
var cachedHashes = NewRandReplKeyCache(kMaxCacheSize)

// authKey returns the bcrypt hash + SHA1 digest of the password.
func authKey(hash []byte, password []byte) (key string) {
	s := sha1.New()
	s.Write(password)
	digest := string(s.Sum(nil))
	key = digest + string(hash)
	return key
}

// compareHashAndPassword is an optimized wrapper around bcrypt.CompareHashAndPassword that
// caches successful results in memory to avoid the _very_ high overhead of calling bcrypt.
func compareHashAndPassword(cache Cache, hash []byte, password []byte) bool {
	// Actually we cache the SHA1 digest of the password to avoid keeping passwords in RAM.
	key := authKey(hash, password)
	if cache.Contains(key) {
		return true
	}
	// Cache missed; now we make the very slow (~100ms) bcrypt call:
	if err := bcrypt.CompareHashAndPassword(hash, password); err != nil {
		// Note: It's important to only cache successful matches, not failures.
		// Failure is supposed to be slow, to make online attacks impractical.
		return false
	}
	cache.Put(key)
	return true
}

// SetBcryptCost will set the bcrypt cost for Sync Gateway to use
// Values of zero or less will use bcryptDefaultCost instead
// An error is returned if the cost is not between bcryptDefaultCost and bcrypt.MaxCost
func (auth *Authenticator) SetBcryptCost(cost int) error {
	if cost <= 0 {
		auth.BcryptCost = DefaultBcryptCost
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "bcrypt cost set to default: %d", cost)
		return nil
	}

	if cost < DefaultBcryptCost || cost > bcrypt.MaxCost {
		return errors.Wrapf(ErrInvalidBcryptCost,
			"%d outside allowed range: %d-%d",
			cost, DefaultBcryptCost, bcrypt.MaxCost)
	}

	base.InfofCtx(auth.LogCtx, base.KeyAuth, "bcrypt cost set to: %d", cost)
	auth.BcryptCost = cost
	auth.bcryptCostChanged = true

	return nil
}

// Cache is an interface to a key only cache.
type Cache interface {

	// Contains returns true if the provided key is present
	// in the cache and false otherwise.
	Contains(key string) bool

	// Len returns the number of keys in the cache.
	Len() int

	// Put adds a key to the cache.
	Put(key string)

	// Purge deletes all items from the cache.
	Purge()
}

// RandReplKeyCache represents a random replacement cache.
type RandReplKeyCache struct {
	size  int                 // Maximum size where this cache can potentially grow upto.
	keys  []string            // A slice of keys for choosing a random key for eviction.
	cache map[string]struct{} // Set of keys for fast lookup.
	lock  sync.RWMutex        // Protects both cache and keys from concurrent access.
}

// Returns a new random replacement key-only cache that can
// potentially grow upto the provided size.
func NewRandReplKeyCache(size int) *RandReplKeyCache {
	return &RandReplKeyCache{
		size:  size,
		cache: make(map[string]struct{}),
	}
}

// Contains returns true if the provided key is present
// in the cache and false otherwise.
func (c *RandReplKeyCache) Contains(key string) (ok bool) {
	c.lock.RLock()
	_, ok = c.cache[key]
	c.lock.RUnlock()
	return ok
}

// Put adds a key to the cache. Eviction occurs when memory is
// over filled or greater than the specified size in the cache.
// Keys are evicted randomly from the cache.
func (c *RandReplKeyCache) Put(key string) {
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
func (c *RandReplKeyCache) Len() int {
	c.lock.RLock()
	length := len(c.cache)
	c.lock.RUnlock()
	return length
}

// Purge deletes all items from the cache.
func (c *RandReplKeyCache) Purge() {
	c.lock.Lock()
	c.keys = nil
	c.cache = map[string]struct{}{}
	c.lock.Unlock()
}
