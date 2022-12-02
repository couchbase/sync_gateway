package js

import (
	"sync"
)

// A thread-safe registry of service TemplateFactory functions. Each is identified by a `serviceID`.
type servicesConfiguration struct {
	mutex    sync.Mutex
	registry []TemplateFactory
}

// Returns the TemplateFactory registered with a serviceID, or nil if not found.
func (config *servicesConfiguration) getService(id serviceID) TemplateFactory {
	config.mutex.Lock()
	defer config.mutex.Unlock()
	if int(id) < len(config.registry) {
		return config.registry[int(id)]
	} else {
		return nil
	}
}

// Registers a new Service, returning its ID.
func (config *servicesConfiguration) addService(factory TemplateFactory) serviceID {
	config.mutex.Lock()
	defer config.mutex.Unlock()
	config.registry = append(config.registry, factory)
	return serviceID(len(config.registry) - 1)
}
