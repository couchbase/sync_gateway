/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"errors"
	"maps"
	"slices"

	sgbucket "github.com/couchbase/sg-bucket"
)

var ErrCollectionsUnsupported = errors.New("collections not supported")

type ScopeAndCollectionName = sgbucket.DataStoreNameImpl

// CollectionNames represent a map of scope names to collection names.
type CollectionNames map[string][]string

// CollectionNameSet respresents a unique set of collection names.
type CollectionNameSet map[string]map[string]struct{}

func DefaultScopeAndCollectionName() ScopeAndCollectionName {
	return ScopeAndCollectionName{Scope: DefaultScope, Collection: DefaultCollection}
}

func NewScopeAndCollectionName(scope, collection string) ScopeAndCollectionName {
	return ScopeAndCollectionName{
		Scope:      scope,
		Collection: collection,
	}
}

type ScopeAndCollectionNames []ScopeAndCollectionName

// ScopeAndCollectionNames returns a dot-separated formatted slice of scope and collection names.
func (s ScopeAndCollectionNames) ScopeAndCollectionNames() []string {
	scopes := make([]string, 0, len(s))
	for _, scopeAndCollection := range s {
		scopes = append(scopes, scopeAndCollection.String())
	}
	return scopes
}

func FullyQualifiedCollectionName(bucketName, scopeName, collectionName string) string {
	return bucketName + "." + scopeName + "." + collectionName
}

// Add adds the provided collections to map. This does not deduplicate collections.
func (c CollectionNames) Add(ds ...sgbucket.DataStoreName) {
	for _, d := range ds {
		if _, ok := c[d.ScopeName()]; !ok {
			c[d.ScopeName()] = []string{d.CollectionName()}
		} else {
			c[d.ScopeName()] = append(c[d.ScopeName()], d.CollectionName())
		}
	}
}

// ToCollectionNameSet converts CollectionNames to a CollectionNameSet.
func (c CollectionNames) ToCollectionNameSet() CollectionNameSet {
	collectionNameSet := make(CollectionNameSet, len(c))
	for scopeName, collections := range c {
		if _, ok := collectionNameSet[scopeName]; !ok {
			collectionNameSet[scopeName] = make(map[string]struct{})
		}
		for _, collection := range collections {
			collectionNameSet[scopeName][collection] = struct{}{}
		}
	}
	return collectionNameSet
}

// NewCollectionNames creates a new CollectionNames from specified collections. Does not deduplicate collections.
func NewCollectionNames(ds ...sgbucket.DataStoreName) CollectionNames {
	c := make(CollectionNames, 1)
	c.Add(ds...)
	for _, collections := range c {
		slices.Sort(collections)
	}
	return c
}

// Add adds the provided collections to map.
func (c CollectionNameSet) Add(ds ...sgbucket.DataStoreName) {
	for _, d := range ds {
		if _, ok := c[d.ScopeName()]; !ok {
			c[d.ScopeName()] = make(map[string]struct{})
		}
		c[d.ScopeName()][d.CollectionName()] = struct{}{}
	}
}

// toCollectionNames converts to CollectionNames, sorting the list of collection names for each scope.
func (c CollectionNameSet) ToCollectionNames() CollectionNames {
	collectionNames := make(CollectionNames, len(c))
	for scopeName, collections := range c {
		if _, ok := collectionNames[scopeName]; !ok {
			collectionNames[scopeName] = make([]string, 0, len(collections))
		}
		names := slices.Collect(maps.Keys(collections))
		slices.Sort(names)
		collectionNames[scopeName] = names
	}
	return collectionNames
}

// NewCollectionNameSet creates a new set from a set of data stores.
func NewCollectionNameSet(ds ...sgbucket.DataStoreName) CollectionNameSet {
	c := make(CollectionNameSet, 1)
	c.Add(ds...)
	return c
}
