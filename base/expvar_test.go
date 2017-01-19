//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"fmt"
	"log"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestRollingMeanExpvar(t *testing.T) {

	rollingMean := NewIntRollingMeanVar(5)
	// Add a few values below capacity, validate mean
	rollingMean.AddValue(2)
	rollingMean.AddValue(4)
	rollingMean.AddValue(6)
	rollingMean.AddValue(8)
	assert.Equals(t, rollingMean.String(), "5")

	// Add a few more to exceed capacity
	rollingMean.AddValue(10)
	rollingMean.AddValue(22)
	assert.Equals(t, rollingMean.String(), "10")

	// Go around the capacity loop a few times to make sure things don't break
	for i := 0; i < 100; i++ {
		rollingMean.AddValue(100)
	}
	assert.Equals(t, rollingMean.String(), "100")

}

func assertMapEntry(t *testing.T, e SequenceTimingExpvar, key string) {
	assertTrue(t, e.timingMap.Get(key) != nil, fmt.Sprintf("Expected map key %s not found", key))
}

func TestTimingExpvarSequenceOnly(t *testing.T) {
	TimingExpvarsEnabled = true
	// Sequence only
	e := NewSequenceTimingExpvar(5, 0, "testSeqOnlyTiming")
	e.UpdateBySequence("SequenceBased", 0, 1)
	e.UpdateBySequence("SequenceBased", 0, 3)
	e.UpdateBySequence("SequenceBased", 0, 4)
	e.UpdateBySequence("SequenceBased", 0, 6)
	e.UpdateBySequence("SequenceBased", 0, 7)
	e.UpdateBySequence("SequenceBased", 0, 10)
	e.UpdateBySequence("SequenceBased", 0, 13)
	e.UpdateBySequence("SequenceBased", 0, 16)

	log.Printf("sequence only: %s", e.String())
	assertMapEntry(t, e, "seq5:SequenceBased")
	assertMapEntry(t, e, "seq10:SequenceBased")
	assertMapEntry(t, e, "seq15:SequenceBased")

}

func TestTimingExpvarRangeOnly(t *testing.T) {
	TimingExpvarsEnabled = true

	// Range only
	e := NewSequenceTimingExpvar(5, 0, "testRangeTiming")
	e.UpdateBySequenceRange("SequenceBased", 0, 0, 3)
	e.UpdateBySequenceRange("SequenceBased", 0, 4, 6)
	e.UpdateBySequenceRange("SequenceBased", 0, 7, 9)
	e.UpdateBySequenceRange("SequenceBased", 0, 10, 12)
	e.UpdateBySequenceRange("SequenceBased", 0, 13, 15)

	assertMapEntry(t, e, "seq5:SequenceBased")
	assertMapEntry(t, e, "seq10:SequenceBased")
	assertMapEntry(t, e, "seq15:SequenceBased")
	log.Printf("range only: %s", e.String())

}

func TestTimingExpvarMixed(t *testing.T) {
	TimingExpvarsEnabled = true

	e := NewSequenceTimingExpvar(5, 0, "testTimingMixed")
	e.UpdateBySequenceRange("Polled", 0, 0, 3)
	e.UpdateBySequenceRange("ChangesNotified", 0, 0, 3)
	e.UpdateBySequence("ChangeEntrySent", 0, 2)
	e.UpdateBySequence("ChangeEntrySent", 0, 3)

	e.UpdateBySequenceRange("Polled", 0, 3, 7)
	e.UpdateBySequenceRange("ChangesNotified", 0, 3, 7)
	e.UpdateBySequence("ChangeEntrySent", 0, 6)
	e.UpdateBySequence("ChangeEntrySent", 0, 7)

	e.UpdateBySequenceRange("Polled", 0, 8, 10)
	e.UpdateBySequenceRange("ChangesNotified", 0, 8, 10)
	e.UpdateBySequence("ChangeEntrySent", 0, 10)

	e.UpdateBySequenceRange("Polled", 0, 11, 18)
	e.UpdateBySequenceRange("ChangesNotified", 0, 11, 18)
	e.UpdateBySequence("ChangeEntrySent", 0, 12)
	e.UpdateBySequence("ChangeEntrySent", 0, 13)
	e.UpdateBySequence("ChangeEntrySent", 0, 16)
	e.UpdateBySequence("ChangeEntrySent", 0, 17)
	e.UpdateBySequence("ChangeEntrySent", 0, 18)

	log.Printf("mixed only: %s", e.String())
	assertMapEntry(t, e, "seq5:Polled")
	assertMapEntry(t, e, "seq5:ChangesNotified")
	assertMapEntry(t, e, "seq5:ChangeEntrySent")
	assertMapEntry(t, e, "seq10:Polled")
	assertMapEntry(t, e, "seq10:ChangesNotified")
	assertMapEntry(t, e, "seq10:ChangeEntrySent")

}
