// notifier.go

package basecouch

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
