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

	sgbucket "github.com/couchbase/sg-bucket"
)

var ErrCollectionsUnsupported = errors.New("collections not supported")

type ScopeAndCollectionName = sgbucket.DataStoreNameImpl

func DefaultScopeAndCollectionName() ScopeAndCollectionName {
	return ScopeAndCollectionName{Scope: DefaultScope, Collection: DefaultCollection}
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
