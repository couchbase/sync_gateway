//  Copyright (c) 2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"strconv"
	"sync"
	"time"
)

const (
	exportDebugExpvars = true
)

// IntMax is an expvar.Value that tracks the maximum value it's given.
type IntMax struct {
	i  int64
	mu sync.RWMutex
}

func (v *IntMax) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return strconv.FormatInt(v.i, 10)
}

func (v *IntMax) SetIfMax(value int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if value > v.i {
		v.i = value
	}
}

// IntMean is an expvar.Value that returns the mean of all values that
// are sent via AddValue or AddSince.
type IntMeanVar struct {
	count int64 // number of values seen
	mean  int64 // average value
	mu    sync.RWMutex
}

func (v *IntMeanVar) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return strconv.FormatInt(v.mean, 10)
}

// Adds value.  Calculates new mean as iterative mean (avoids int overflow)
func (v *IntMeanVar) AddValue(value int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.count++
	v.mean = v.mean + int64((value-v.mean)/v.count)
}

func (v *IntMeanVar) AddSince(start time.Time) {
	v.AddValue(time.Since(start).Nanoseconds())
}

type DebugIntMeanVar struct {
	v IntMeanVar
}

func (d *DebugIntMeanVar) String() string {
	if exportDebugExpvars {
		return d.v.String()
	}
	return ""
}

func (d *DebugIntMeanVar) AddValue(value int64) {
	if exportDebugExpvars {
		d.v.AddValue(value)
	}
}

func (d *DebugIntMeanVar) AddSince(start time.Time) {
	if exportDebugExpvars {
		d.v.AddSince(start)
	}
}
