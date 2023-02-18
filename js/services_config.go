/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package js

import (
	"sync"
)

// A thread-safe registry of Services. Each is identified by a `serviceID`.
type servicesConfiguration struct {
	mutex    sync.Mutex
	registry []*Service
}

// Registers a new Service, assigning its `id` field.
func (config *servicesConfiguration) addService(service *Service) {
	config.mutex.Lock()
	defer config.mutex.Unlock()

	config.registry = append(config.registry, service)
	service.id = serviceID(len(config.registry) - 1)
}

// Checks that the registry contains this Service instance.
func (config *servicesConfiguration) hasService(service *Service) bool {
	config.mutex.Lock()
	defer config.mutex.Unlock()

	return int(service.id) < len(config.registry) && service == config.registry[service.id]
}

// Returns the [first] Service with a given name, or nil if not found.
func (config *servicesConfiguration) findServiceNamed(name string) *Service {
	config.mutex.Lock()
	defer config.mutex.Unlock()

	for _, service := range config.registry {
		if service.name == name {
			return service
		}
	}
	return nil
}
