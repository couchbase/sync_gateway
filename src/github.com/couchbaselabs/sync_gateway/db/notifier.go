//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"sync"
)

var gConditions = make(map[string]*sync.Cond)
var gConditionsLock sync.Mutex

func conditionNamed(name string) *sync.Cond {
	gConditionsLock.Lock()
	defer gConditionsLock.Unlock()

	return gConditions[name]
}

func makeConditionNamed(name string) *sync.Cond {
	gConditionsLock.Lock()
	defer gConditionsLock.Unlock()

	c := gConditions[name]
	if c == nil {
		c = sync.NewCond(&sync.Mutex{})
		gConditions[name] = c
	}
	return c
}

func waitFor(name string) {
	c := makeConditionNamed(name)
	c.L.Lock()
	defer c.L.Unlock()
	c.Wait()
}

func notify(name string) {
	c := conditionNamed(name)
	if c != nil {
		c.Broadcast()
	}
}
