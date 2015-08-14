//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"strconv"
	"sync"
)

const (
	KMaxVbNo           = 1024 // TODO: load from cluster config
	KStableSequenceKey = "_cache_stableSeq"
)

type SequenceClock interface {
	SetSequence(vbNo uint16, vbSequence uint64)    // Sets the sequence value for a vbucket
	SetMaxSequence(vbNo uint16, vbSequence uint64) // Sets the sequence value for a vbucket - must be larger than existing sequence
	GetSequence(vbNo uint16) (vbSequence uint64)   // Retrieves the sequence value for a vbucket
	Cas() (casOut uint64)                          // Gets the last known cas for this sequence clock
	SetCas(cas uint64)                             // Sets the last known cas for this sequence clock
	Marshal() (value []byte, err error)            // Marshals the sequence value
	Unmarshal(value []byte) error                  // Unmarshals the sequence value
	UpdateWithClock(updateClock SequenceClock)     // Updates the clock with values from updateClock
	Value() map[uint16]uint64                      // Returns the raw vector clock
	HashedValue() string                           // Returns previously hashed value, if present.  If not present, does NOT generate hash
	Equals(otherClock SequenceClock) bool          // Evaluates whether two clocks are identical
	AllAfter(otherClock SequenceClock) bool        // True if all entries in clock are greater than or equal to the corresponding values in otherClock
	AllBefore(otherClock SequenceClock) bool       // True if all entries in clock are less than or equal to the corresponding values in otherClock
	AnyAfter(otherClock SequenceClock) bool        // True if any entries in clock are greater than the corresponding values in otherClock
	AnyBefore(otherClock SequenceClock) bool       // True if any entries in clock are less than the corresponding values in otherClock
}

// Vector-clock based sequence.  Not thread-safe - use SyncSequenceClock for usages with potential for concurrent access.
type SequenceClockImpl struct {
	value       map[uint16]uint64
	hashedValue string
	cas         uint64
}

func NewSequenceClockImpl() *SequenceClockImpl {
	// Initialize empty clock
	clock := &SequenceClockImpl{
		value: make(map[uint16]uint64, KMaxVbNo),
	}
	return clock
}

func NewSequenceClockFromHash(hashedValue string) *SequenceClockImpl {

	log.Println("New sequence clock from hash:", hashedValue)
	clock := NewSequenceClockImpl()
	clock.hashedValue = hashedValue
	// TODO: resolve hash.  Currently returns a clock with all vbuckets set to int value
	seqInt, err := strconv.ParseUint(hashedValue, 0, 64)
	if err == nil {
		for i := 0; i < 1024; i++ {
			clock.SetSequence(uint16(i), seqInt)
		}
	}

	return clock
}

func NewSequenceClockForBytes(bytes []byte) (*SequenceClockImpl, error) {
	clock := NewSequenceClockImpl()
	err := clock.Unmarshal(bytes)
	return clock, err
}

func (c *SequenceClockImpl) SetSequence(vbNo uint16, vbSequence uint64) {
	c.value[vbNo] = vbSequence
}

func (c *SequenceClockImpl) SetMaxSequence(vbNo uint16, vbSequence uint64) {
	if c.value[vbNo] <= vbSequence {
		c.value[vbNo] = vbSequence
	} else {
		Warn("Attempted to lower sequence value when calling SetMaxSequence")
	}
}

func (c *SequenceClockImpl) GetSequence(vbNo uint16) (vbSequence uint64) {
	return c.value[vbNo]
}

// Copies a channel clock
func (c *SequenceClockImpl) clone() SequenceClockImpl {
	copy := SequenceClockImpl{
		value: make(map[uint16]uint64, len(c.value)),
		cas:   c.cas,
	}

	for k, v := range c.value {
		copy.value[k] = v
	}
	return copy
}

func (c *SequenceClockImpl) Cas() uint64 {
	return c.cas
}

func (c *SequenceClockImpl) SetCas(cas uint64) {
	c.cas = cas
}

func (c *SequenceClockImpl) Value() map[uint16]uint64 {
	return c.value
}

// TODO: replace with something more intelligent than gob encode, to take advantage of known
//       clock structure?
func (c *SequenceClockImpl) Marshal() ([]byte, error) {
	var output bytes.Buffer
	enc := gob.NewEncoder(&output)
	err := enc.Encode(c.value)
	if err != nil {
		return nil, err
	}

	return output.Bytes(), nil
}

func (c *SequenceClockImpl) Unmarshal(value []byte) error {

	input := bytes.NewBuffer(value)
	dec := gob.NewDecoder(input)
	err := dec.Decode(&c.value)
	if err != nil {
		return err
	}
	return nil
}

// Compares another sequence clock with this one
func (c *SequenceClockImpl) Equals(other SequenceClock) bool {

	if c.hashEquals(other.HashedValue()) {
		return true
	}
	for vb, sequence := range other.Value() {
		if sequence != c.value[vb] {
			return false
		}
	}
	return true
}

// Compares another sequence clock with this one.  Returns true only if ALL vb values in the clock
// are greater than or equal to corresponding values in other
func (c *SequenceClockImpl) AllAfter(other SequenceClock) bool {

	for vb, sequence := range other.Value() {
		if sequence > c.value[vb] {
			return false
		}
	}
	return true
}

// Compares another sequence clock with this one.  Returns true only if ALL vb values in
// the clock are less than or equal to the corresponding values in other
func (c *SequenceClockImpl) AllBefore(other SequenceClock) bool {

	for vb, sequence := range other.Value() {
		if sequence < c.value[vb] {
			return false
		}
	}
	return true
}

// Compares another sequence clock with this one.  Returns true if ANY vb values in
// the clock are less than the corresponding values in other
func (c *SequenceClockImpl) AnyBefore(other SequenceClock) bool {

	if c.hashEquals(other.HashedValue()) {
		return false
	}
	for vb, sequence := range other.Value() {
		if c.value[vb] < sequence {
			return true
		}
	}
	return false
}

// Compares another sequence clock with this one.  Returns true if ANY vb values in the clock
// are greater than the corresponding values in other
func (c *SequenceClockImpl) AnyAfter(other SequenceClock) bool {

	if c.hashEquals(other.HashedValue()) {
		return false
	}
	for vb, sequence := range other.Value() {
		if c.value[vb] > sequence {
			return true
		}
	}
	return false
}

// Deep-copies a SequenceClock
func (c *SequenceClockImpl) Copy() SequenceClock {
	result := &SequenceClockImpl{}
	for key, value := range c.value {
		result.value[key] = value
	}
	return result
}

func (c *SequenceClockImpl) HashedValue() string {
	// TBD
	return "hash-0"
}

func (c *SequenceClockImpl) hashEquals(otherHash string) bool {
	if otherHash != "" && c.hashedValue != "" && otherHash == c.hashedValue {
		return true
	}
	return false
}

// Compares another sequence clock with this one, and returns the set of vbucket ids where
// the other bucket has a higher sequence value.  Used during Since calculations to identify which
// vbuckets need to be retrieved.
func (c *SequenceClockImpl) findModified(other SequenceClock) (modified []uint16) {
	if c.hashEquals(other.HashedValue()) {
		return nil
	}
	for vb, sequence := range other.Value() {
		if sequence > c.value[vb] {
			modified = append(modified, vb)
		}
	}
	return modified
}

func (c *SequenceClockImpl) UpdateWithClock(updateClock SequenceClock) {
	for vb, sequence := range updateClock.Value() {
		if sequence > 0 {
			// TODO: This method assumes the non-zero values in updateClock are greater than
			//  the current clock values.  The following check is for safety/testing during
			//  implementation - could consider removing for performance
			currentSequence, ok := c.value[vb]
			if ok && sequence < currentSequence {
				panic(fmt.Sprintf("Update attempted to set clock to earlier sequence.  Vb: %d, currentSequence: %d, newSequence: %d", vb, currentSequence, sequence))
			}
			c.value[vb] = sequence
		}
	}
	LogTo("DIndex+", "UpdateWithClock completed - updated clock to:%s", PrintClock(c))
}

// Synchronized Sequence Clock - should be used in shared usage scenarios
type SyncSequenceClock struct {
	Clock *SequenceClockImpl
	lock  sync.RWMutex
}

func NewSyncSequenceClock() *SyncSequenceClock {
	// Initialize empty clock
	syncClock := SyncSequenceClock{}
	syncClock.Clock = NewSequenceClockImpl()
	return &syncClock
}

func (c *SyncSequenceClock) Cas() uint64 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.Clock.Cas()
}

func (c *SyncSequenceClock) SetCas(cas uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.Clock.SetCas(cas)
}

func (c *SyncSequenceClock) SetSequence(vbNo uint16, sequence uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.Clock.SetSequence(vbNo, sequence)
}

func (c *SyncSequenceClock) SetMaxSequence(vbNo uint16, vbSequence uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.Clock.SetMaxSequence(vbNo, vbSequence)
}
func (c *SyncSequenceClock) GetSequence(vbNo uint16) (sequence uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.Clock.GetSequence(vbNo)
}

func (c *SyncSequenceClock) HashedValue() string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.Clock.HashedValue()
}

func (c *SyncSequenceClock) AllAfter(other SequenceClock) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.Clock.AllAfter(other)
}

func (c *SyncSequenceClock) AllBefore(other SequenceClock) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.Clock.AllAfter(other)
}

func (c *SyncSequenceClock) AnyAfter(other SequenceClock) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.Clock.AllAfter(other)
}

func (c *SyncSequenceClock) AnyBefore(other SequenceClock) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.Clock.AllAfter(other)
}

func (c *SyncSequenceClock) Equals(other SequenceClock) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.Clock.Equals(other)
}

// Copies a channel clock
func (c *SyncSequenceClock) clone() *SyncSequenceClock {
	c.lock.RLock()
	defer c.lock.RUnlock()
	clockCopy := c.Clock.clone()
	return &SyncSequenceClock{
		Clock: &clockCopy,
	}
}

func (c *SyncSequenceClock) Value() map[uint16]uint64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.Clock.Value()
}

// TODO: possibly replace with something more intelligent than gob encode, to take advantage of known
//       clock structure?
func (c *SyncSequenceClock) Marshal() ([]byte, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.Clock.Marshal()
}

func (c *SyncSequenceClock) Unmarshal(value []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.Clock.Unmarshal(value)
}

func (c *SyncSequenceClock) UpdateWithClock(updateClock SequenceClock) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.Clock.UpdateWithClock(updateClock)
}

// Clock utility functions
func PrintClock(clock SequenceClock) string {
	var output string
	output += fmt.Sprintf("[cas:%d]", clock.Cas())
	for vbNo, sequence := range clock.Value() {
		if sequence > 0 {
			output += fmt.Sprintf("[%d:%d]", vbNo, sequence)
		}
	}
	return output
}

func GetMinimumClock(a SequenceClock, b SequenceClock) *SequenceClockImpl {
	minClock := NewSequenceClockImpl()
	// Need to iterate over all index values instead of using range, to handle map entries in b that
	// are not in a (and vice versa)
	for i := uint16(0); i < KMaxVbNo; i++ {
		aValue := a.GetSequence(i)
		bValue := b.GetSequence(i)
		if aValue < bValue {
			minClock.SetSequence(i, bValue)
		} else {
			if aValue > 0 {
				minClock.SetSequence(i, aValue)
			}
		}
	}
	return minClock
}
