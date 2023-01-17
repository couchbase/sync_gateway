package js

import (
	"sync"
)

// A thread-safe registry of service TemplateFactory functions. Each is identified by a `serviceID`.
type servicesConfiguration struct {
	mutex    sync.Mutex
	registry []TemplateFactory
	names    []string
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

// Looks up a service by name.
func (config *servicesConfiguration) findService(name string) (serviceID, bool) {
	config.mutex.Lock()
	defer config.mutex.Unlock()
	for i, str := range config.names {
		if str == name {
			return serviceID(i), true
		}
	}
	return serviceID(0), false
}

// Registers a new Service, returning its ID.
func (config *servicesConfiguration) addService(factory TemplateFactory, name string) serviceID {
	config.mutex.Lock()
	defer config.mutex.Unlock()
	config.registry = append(config.registry, factory)
	config.names = append(config.names, name)
	return serviceID(len(config.registry) - 1)
}
