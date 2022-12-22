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

type ScopeAndCollectionName struct {
	Scope      string
	Collection string
}

var _ sgbucket.DataStoreName = &ScopeAndCollectionName{}

func (s ScopeAndCollectionName) ScopeName() string {
	return s.Scope
}

func (s ScopeAndCollectionName) CollectionName() string {
	return s.Collection
}
