// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"errors"
	"fmt"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/require"
)

func TestWriteTombstoneWithXattrs(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	col := bucket.DefaultDataStore()

	type casOption uint32

	const (
		zeroCas casOption = iota
		fakeCas
		previousCas
	)
	type testCase struct {
		name           string
		previousDoc    *sgbucket.BucketDocument
		deleteBody     bool
		finalBody      []byte
		finalXattrs    map[string][]byte
		updatedXattrs  map[string][]byte
		xattrsToDelete []string
		cas            casOption
		writeErrorFunc func(testing.TB, error)
	}
	tests := []testCase{
		// alive document with xattrs
		/* CBG-3918, should be a cas mismatch error
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr1,cas=0,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:        zeroCas,
			deleteBody: true,
			writeErrorFunc: requireCasMismatchError,
		},
		*/
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr1,cas=1,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            fakeCas,
			deleteBody:     true,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr1,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:        previousCas,
			deleteBody: true,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
		},
		/* CBG-3918, should be a cas mismatch error
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr2,cas=0,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:        zeroCas,
			deleteBody: true,
			writeErrorFunc: requireCasMismatchError,
		},
		*/
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr2,cas=1,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            fakeCas,
			deleteBody:     true,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr2,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:        previousCas,
			deleteBody: true,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
				"_xattr2": []byte(`{"c" : "d"}`),
			},
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr1,cas=0,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            zeroCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr1,cas=1,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            fakeCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr1,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			finalBody: []byte(`{"foo": "bar"}`),
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:        previousCas,
			deleteBody: false,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr2,cas=0,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            zeroCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr2,cas=1,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            fakeCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body+_xattr1,updatedXattrs=_xattr2,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:        previousCas,
			deleteBody: false,
			finalBody:  []byte(`{"foo": "bar"}`),
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
				"_xattr2": []byte(`{"c" : "d"}`),
			},
		},
		// alive document without xattrs
		/* CBG-3918, should be a cas mismatch error
		{
			name: "previousDoc=body,updatedXattrs=_xattr1,cas=0,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:        zeroCas,
			deleteBody: true,
			writeErrorFunc: requireCasMismatchError,
		},
		*/
		{
			name: "previousDoc=body,updatedXattrs=_xattr1,cas=1,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:            fakeCas,
			deleteBody:     true,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body,updatedXattrs=_xattr1,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:        previousCas,
			deleteBody: true,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
		},
		{
			name: "previousDoc=body,updatedXattrs=_xattr1,cas=0,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:            zeroCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body,updatedXattrs=_xattr1,cas=1,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:            fakeCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=body,updatedXattrs=_xattr1,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:        previousCas,
			deleteBody: false,
			finalBody:  []byte(`{"foo": "bar"}`),
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
		},
		// tombstone
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr1,cas=0,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            zeroCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundOrCasMismatchError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr1,cas=1,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            fakeCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundOrCasMismatchError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr1,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            previousCas,
			deleteBody:     true,
			writeErrorFunc: requireDocNotFoundOrCasMismatchError,
		},

		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr1,cas=0,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            zeroCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr1,cas=1,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:            fakeCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr1,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
			cas:        previousCas,
			deleteBody: false,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c" : "d"}`),
			},
		},

		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr2,cas=0,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            zeroCas,
			deleteBody:     true,
			writeErrorFunc: RequireDocNotFoundError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr2,cas=1,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            fakeCas,
			deleteBody:     true,
			writeErrorFunc: RequireDocNotFoundError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr2,cas=correct,deleteBody=true",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            previousCas,
			deleteBody:     true,
			writeErrorFunc: RequireDocNotFoundError,
		},

		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr2,cas=0,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            zeroCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr2,cas=1,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:            fakeCas,
			deleteBody:     false,
			writeErrorFunc: requireCasMismatchError,
		},
		{
			name: "previousDoc=tombstone+_xattr1,updatedXattrs=_xattr2,cas=correct,deleteBody=false",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr2": []byte(`{"c" : "d"}`),
			},
			cas:        previousCas,
			deleteBody: false,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
				"_xattr2": []byte(`{"c" : "d"}`),
			},
		},
		// nodoc
		{
			name: "previousDoc=nodoc,updatedXattrs=_xattr1,cas=0,deleteBody=true",
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:            zeroCas,
			deleteBody:     true,
			writeErrorFunc: RequireDocNotFoundError,
		},
		{
			name: "previousDoc=nodoc,updatedXattrs=_xattr1,cas=1,deleteBody=true",
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:            fakeCas,
			deleteBody:     true,
			writeErrorFunc: RequireDocNotFoundError,
		},
		{
			name: "previousDoc=nodoc,updatedXattrs=_xattr1,cas=0,deleteBody=false",
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:        zeroCas,
			deleteBody: false,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
		},
		{
			name: "previousDoc=nodoc,updatedXattrs=_xattr1,cas=1,deleteBody=false",
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
			cas:            fakeCas,
			writeErrorFunc: RequireDocNotFoundError,
		},
	}
	if bucket.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations) {
		tests = append(tests, []testCase{
			// alive document with multiple xattrs
			/* CBG-3918, should be a cas mismatch error
			{
				name: "previousDoc=body+_xattr1,xattrsToUpdate=_xattr1+_xattr2,cas=0,deleteBody=true",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
					},
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:        zeroCas,
				deleteBody: true,
				writeErrorFunc: requireCasMismatchError,
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
			},
			*/
			{
				name: "previousDoc=body+_xattr1,xattrsToUpdate=_xattr1+_xattr2,cas=1,deleteBody=true",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
					},
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:            fakeCas,
				deleteBody:     true,
				writeErrorFunc: requireCasMismatchError,
			},
			{
				name: "previousDoc=body+_xattr1,xattrsToUpdate=_xattr1+_xattr2,cas=correct,deleteBody=true",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
					},
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:        previousCas,
				deleteBody: true,
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
			},
			{
				name: "previousDoc=body+_xattr1+_xattr2,xattrsToUpdate=_xattr1+_xattr2,cas=0,deleteBody=false",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
					},
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:            zeroCas,
				deleteBody:     false,
				writeErrorFunc: requireCasMismatchError,
			},
			{
				name: "previousDoc=body+_xattr1+_xattr2,xattrsToUpdate=_xattr1+_xattr2,cas=1,deleteBody=false",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
					},
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:            fakeCas,
				deleteBody:     false,
				writeErrorFunc: requireCasMismatchError,
			},
			{
				name: "previousDoc=body+_xattr1+_xattr2,xattrsToUpdate=_xattr1+_xattr2,cas=correct,deleteBody=false",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
					},
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:        previousCas,
				deleteBody: false,
				finalBody:  []byte(`{"foo": "bar"}`),
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
			},
			// alive document with no xattrs
			/* CBG-3918, should be a cas mismatch error
			{
				name: "previousDoc=body,xattrsToUpdate=_xattr1+xattr2,cas=0,deleteBody=true",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:        zeroCas,
				deleteBody: true,
				writeErrorFunc: requireCasMismatchError,
				},
			},
			*/
			{
				name: "previousDoc=body,xattrsToUpdate=_xattr1+xattr2,cas=1,deleteBody=true",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:            fakeCas,
				deleteBody:     true,
				writeErrorFunc: requireCasMismatchError,
			},
			{
				name: "previousDoc=body,xattrsToUpdate=_xattr1,cas=correct,deleteBody=true",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:        previousCas,
				deleteBody: true,
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
			},
			{
				name: "previousDoc=body,xattrsToUpdate=_xattr1,cas=0,deleteBody=false",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:            zeroCas,
				deleteBody:     false,
				writeErrorFunc: requireCasMismatchError,
			},
			{
				name: "previousDoc=body,xattrsToUpdate=_xattr1,cas=1,deleteBody=false",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:            fakeCas,
				deleteBody:     false,
				writeErrorFunc: requireCasMismatchError,
			},
			{
				name: "previousDoc=body,xattrsToUpdate=_xattr1,cas=correct,deleteBody=false",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:        previousCas,
				deleteBody: false,
				finalBody:  []byte(`{"foo": "bar"}`),
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
			},
			// tombstone with multiple xattrs
			{
				name:        "previousDoc=nil,xattrsToUpdate=_xattr1+_xattr2,cas=0,deleteBody=true",
				previousDoc: nil,
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:            zeroCas,
				deleteBody:     true,
				writeErrorFunc: RequireDocNotFoundError,
			},
			{
				name:        "previousDoc=nil,xattrsToUpdate=_xattr1+xattr2,cas=1,deleteBody=true",
				previousDoc: nil,
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:            fakeCas,
				deleteBody:     true,
				writeErrorFunc: RequireDocNotFoundError,
			},
			{
				name:        "previousDoc=nil,xattrsToUpdate=_xattr1,cas=0,deleteBody=false",
				previousDoc: nil,
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:        zeroCas,
				deleteBody: false,
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
			},
			{
				name:        "previousDoc=nil,xattrsToUpdate=_xattr1,cas=1,deleteBody=false",
				previousDoc: nil,
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				cas:            fakeCas,
				deleteBody:     false,
				writeErrorFunc: RequireDocNotFoundError,
			},
			{
				name:        "previousDoc=nil,xattrsToUpdate=_xattr1,xattrsToDelete=_xattr2,cas=0,deleteBody=false",
				previousDoc: nil,
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
				},
				xattrsToDelete: []string{"_xattr2"},
				cas:            zeroCas,
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
				},
			},
			{
				name:        "previousDoc=nil,xattrsToUpdate=_xattr1,xattrsToDelete=_xattr2,cas=0,deleteBody=true",
				previousDoc: nil,
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
				},
				xattrsToDelete: []string{"_xattr2"},
				cas:            zeroCas,
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
				},
				deleteBody:     true,
				writeErrorFunc: RequireDocNotFoundError,
			},
			{
				name: "previousDoc=body+_xattr1,_xattr2,xattrsToUpdate=_xattr1,xattrsToDelete=_xattr2,cas=correct,deleteBody=false",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
					},
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
				},
				xattrsToDelete: []string{"_xattr2"},
				cas:            previousCas,
				deleteBody:     false,
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
				},
				finalBody: []byte(`{"foo": "bar"}`),
			},
			{
				name: "previousDoc=body+_xattr1,_xattr2,xattrsToUpdate=_xattr1,xattrsToDelete=_xattr2,cas=correct,deleteBody=true",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
					},
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
				},
				xattrsToDelete: []string{"_xattr2"},
				cas:            previousCas,
				deleteBody:     true,
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
				},
			},
		}...)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var exp uint32
			docID := t.Name()
			cas := uint64(0)
			if test.cas == fakeCas {
				cas = 1
			}
			if test.previousDoc != nil {
				var casOut uint64
				var err error
				if test.previousDoc.Body == nil {
					casOut, err = col.WriteTombstoneWithXattrs(ctx, docID, exp, 0, test.previousDoc.Xattrs, nil, false, nil)
				} else {
					casOut, err = col.WriteWithXattrs(ctx, docID, exp, 0, test.previousDoc.Body, test.previousDoc.Xattrs, nil, nil)
				}
				require.NoError(t, err)
				if test.cas == previousCas {
					cas = casOut
				}
			}
			_, err := col.WriteTombstoneWithXattrs(ctx, docID, exp, cas, test.updatedXattrs, nil, test.deleteBody, nil)
			if test.writeErrorFunc != nil {
				test.writeErrorFunc(t, err)
				if test.finalBody != nil {
					require.Fail(t, "finalBody should not be set when expecting an error")
				}
				return
			}
			require.NoError(t, err)

			xattrKeys := make([]string, 0, len(test.updatedXattrs))
			for k := range test.updatedXattrs {
				xattrKeys = append(xattrKeys, k)
			}
			if test.previousDoc != nil {
				for xattrKey := range test.previousDoc.Xattrs {
					_, ok := test.updatedXattrs[xattrKey]
					if !ok {
						xattrKeys = append(xattrKeys, xattrKey)
					}
				}
			}

			body, xattrs, _, err := col.GetWithXattrs(ctx, docID, xattrKeys)
			require.NoError(t, err)
			if test.finalBody == nil || UnitTestUrlIsWalrus() {
				require.Equal(t, "", string(body))
			} else {
				require.JSONEq(t, string(test.finalBody), string(body))
			}
			requireXattrsEqual(t, test.finalXattrs, xattrs)
		})
	}
}

func TestWriteUpdateWithXattrs(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	col := bucket.DefaultDataStore()

	type testCase struct {
		name         string
		previousDoc  *sgbucket.BucketDocument
		updatedDoc   sgbucket.UpdatedDoc
		finalBody    []byte
		finalXattrs  map[string][]byte
		errorsIs     error
		errorFunc    func(testing.TB, error)
		getErrorFunc func(testing.TB, error)
	}
	tests := []testCase{
		// alive document with xattrs
		/* this fails in rosmar with a getError and doesn't write null doc body
		{
			name:      "previousDoc=nil,updatedDoc=nil",
			finalBody: []byte("null"),
		},
		*/
		{
			name: "previousDoc=body+_xattr1,updatedDoc=nil",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			errorsIs: sgbucket.ErrNeedXattrs,
		},
		/* This should fail, and does under rosmar but not CBS
		{
			name: "previousDoc=_xattr1,updatedDoc=nil",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
		},
		*/
		{
			name: "previousDoc=nil,updatedDoc=_xattr1",
			updatedDoc: sgbucket.UpdatedDoc{
				Xattrs:      map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
				IsTombstone: true,
			},
			finalXattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
		},
		{
			name: "previousDoc=body+_xattr1,updatedDoc=_xattr1",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedDoc: sgbucket.UpdatedDoc{
				Xattrs:      map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
				IsTombstone: true,
			},
			finalXattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
		},
		{
			name: "previousDoc=_xattr1,updatedDoc=_xattr1",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedDoc: sgbucket.UpdatedDoc{
				Xattrs:      map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
				IsTombstone: true,
			},
			finalXattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
		},
	}

	if bucket.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations) {
		tests = append(tests, []testCase{
			{
				name: "previousDoc=body+_xattr1,_xattr2,updatedDoc=_xattr1",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
						"_xattr2": []byte(`{"e" : "f"}`),
					},
				},
				updatedDoc: sgbucket.UpdatedDoc{
					Xattrs:      map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
					IsTombstone: true,
				},
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c" : "d"}`),
					"_xattr2": []byte(`{"e" : "f"}`),
				},
			},
			{
				name: "previousDoc=_xattr1,xattr2,updatedDoc=_xattr1",
				previousDoc: &sgbucket.BucketDocument{
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
						"_xattr2": []byte(`{"e" : "f"}`),
					},
				},
				updatedDoc: sgbucket.UpdatedDoc{
					Xattrs:      map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
					IsTombstone: true,
				},
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c" : "d"}`),
					"_xattr2": []byte(`{"e" : "f"}`),
				},
			},
			{
				name: "previousDoc=_xattr1,xattr2,updatedDoc=tombstone+_xattr1",
				previousDoc: &sgbucket.BucketDocument{
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
						"_xattr2": []byte(`{"e" : "f"}`),
					},
				},
				updatedDoc: sgbucket.UpdatedDoc{
					IsTombstone: true,
					Xattrs:      map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)}},
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c" : "d"}`),
					"_xattr2": []byte(`{"e" : "f"}`),
				},
			},
		}...)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			docID := t.Name()
			if test.previousDoc != nil {
				var exp uint32
				var cas uint64
				var mutateInOptions *sgbucket.MutateInOptions
				if len(test.previousDoc.Body) == 0 {
					deleteBody := false
					var mutateInOptions *sgbucket.MutateInOptions
					_, err := col.WriteTombstoneWithXattrs(ctx, docID, exp, cas, test.previousDoc.Xattrs, nil, deleteBody, mutateInOptions)
					require.NoError(t, err)
				} else {
					_, err := col.WriteWithXattrs(ctx, docID, exp, cas, test.previousDoc.Body, test.previousDoc.Xattrs, nil, mutateInOptions)
					require.NoError(t, err)
				}
			}
			writeUpdateFunc := func(_ []byte, _ map[string][]byte, _ uint64) (sgbucket.UpdatedDoc, error) {
				return test.updatedDoc, nil
			}

			cas, err := col.WriteUpdateWithXattrs(ctx, docID, nil, 0, nil, nil, writeUpdateFunc)
			if test.errorsIs != nil {
				require.ErrorIs(t, err, test.errorsIs)
				require.Equal(t, uint64(0), cas)
				return
			} else if test.errorFunc != nil {
				test.errorFunc(t, err)
				require.Equal(t, uint64(0), cas)
				return
			}
			require.NoError(t, err)
			require.NotEqual(t, uint64(0), cas)

			// assemble names of any possible xattrs
			xattrNames := make([]string, 0, len(test.updatedDoc.Xattrs))
			for k := range test.updatedDoc.Xattrs {
				xattrNames = append(xattrNames, k)
			}
			if test.previousDoc != nil {
				for k := range test.previousDoc.Xattrs {
					if _, ok := test.updatedDoc.Xattrs[k]; !ok {
						xattrNames = append(xattrNames, k)
					}
				}
			}
			body, xattrs, _, err := col.GetWithXattrs(ctx, docID, xattrNames)
			if test.getErrorFunc != nil {
				test.getErrorFunc(t, err)
				return
			}
			require.NoError(t, err)
			if UnitTestUrlIsWalrus() && test.updatedDoc.IsTombstone {
				require.Equal(t, "", string(body))
			} else if len(test.finalBody) > 0 {
				require.JSONEq(t, string(test.finalBody), string(body))
			} else {
				require.Equal(t, string(test.finalBody), string(body))
			}
			requireXattrsEqual(t, test.finalXattrs, xattrs)
		})
	}
}

func TestWriteWithXattrs(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	col := bucket.DefaultDataStore()

	type testCase struct {
		name           string
		body           []byte
		cas            uint64
		xattrs         map[string][]byte
		xattrsToDelete []string
		errorIs        error
		errorFunc      func(testing.TB, error)
	}

	tests := []testCase{
		{
			name: "body=true,xattrs=nil,xattrsToDelete=nil,cas=0",
			body: []byte(`{"foo": "bar"}`),
		},
		{
			name:      "body=true,xattrs=nil,xattrsToDelete=nil,cas=1",
			body:      []byte(`{"foo": "bar"}`),
			cas:       uint64(1),
			errorFunc: requireDocNotFoundOrCasMismatchError,
		},
		{
			name:           "body=true,xattrs=nil,xattrsToDelete=xattr1,cas=0",
			body:           []byte(`{"foo": "bar"}`),
			xattrsToDelete: []string{"xattr1"},
			errorIs:        sgbucket.ErrDeleteXattrOnDocumentInsert,
		},
		{
			name:           "body=true,xattrs=nil,xattrsToDelete=xattr1,cas=1",
			body:           []byte(`{"foo": "bar"}`),
			xattrsToDelete: []string{"xattr1"},
			cas:            uint64(1),
			errorFunc:      requireDocNotFoundOrCasMismatchError,
		},
		{
			name:           "body=true,xattrs=xattr1,xattrsToDelete=xattr1,cas=0",
			xattrs:         map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)},
			body:           []byte(`{"foo": "bar"}`),
			xattrsToDelete: []string{"xattr1"},
			errorIs:        sgbucket.ErrDeleteXattrOnDocumentInsert,
		},
		{
			name:           "body=true,xattrs=xattr1,xattrsToDelete=xattr1,cas=1",
			body:           []byte(`{"foo": "bar"}`),
			xattrs:         map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)},
			xattrsToDelete: []string{"xattr1"},
			cas:            uint64(1),
			errorIs:        sgbucket.ErrUpsertAndDeleteSameXattr,
		},
		{
			name:   "body=true,xattrs=xattr1,xattrsToDelete=nil,cas=0",
			body:   []byte(`{"foo": "bar"}`),
			xattrs: map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)},
		},
		{
			name:      "body=true,xattrs=xattr1,xattrsToDelete=nil,cas=1",
			body:      []byte(`{"foo": "bar"}`),
			xattrs:    map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)},
			cas:       uint64(1),
			errorFunc: requireDocNotFoundOrCasMismatchError,
		},
		{
			name:    "xattr_nil_value",
			body:    []byte(`{"foo": "bar"}`),
			xattrs:  map[string][]byte{"xattr1": nil},
			errorIs: sgbucket.ErrNilXattrValue,
		},
	}
	if bucket.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations) {
		tests = append(tests, []testCase{
			{
				name:           "body=true,xattrs=nil,xattrsToDelete=xattr1,xattr2,cas=0",
				body:           []byte(`{"foo": "bar"}`),
				xattrsToDelete: []string{"xattr1", "xattr2"},
				errorIs:        sgbucket.ErrDeleteXattrOnDocumentInsert,
			},
			{
				name:           "body=true,xattrs=nil,xattrsToDelete=xattr1,xattr2,cas=1",
				body:           []byte(`{"foo": "bar"}`),
				xattrsToDelete: []string{"xattr1", "xattr2"},
				cas:            uint64(1),
				errorFunc:      requireDocNotFoundOrCasMismatchError,
			},
			{
				name:           "body=true,xattrs=xattr1,xattrsToDelete=xattr1,xattr2,cas=0",
				body:           []byte(`{"foo": "bar"}`),
				xattrs:         map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)},
				xattrsToDelete: []string{"xattr1", "xattr2"},
				errorIs:        sgbucket.ErrDeleteXattrOnDocumentInsert,
			},

			{
				name:           "body=true,xattrs=xattr1,xattrsToDelete=xattr1,xattr2,cas=1",
				body:           []byte(`{"foo": "bar"}`),
				xattrs:         map[string][]byte{"xattr1": []byte(`{"a" : "b"}`)},
				xattrsToDelete: []string{"xattr1", "xattr2"},
				cas:            uint64(1),
				errorIs:        sgbucket.ErrUpsertAndDeleteSameXattr,
			},
		}...)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.errorFunc != nil && test.errorIs != nil {
				require.FailNow(t, "test case should specify errFunc  xor errorIs")
			}
			docID := t.Name()
			cas, err := col.WriteWithXattrs(ctx, docID, 0, test.cas, test.body, test.xattrs, test.xattrsToDelete, nil)
			if test.errorFunc != nil {
				test.errorFunc(t, err)
				require.Equal(t, uint64(0), cas)
				return
			}
			require.ErrorIs(t, err, test.errorIs)
			if test.errorIs != nil {
				require.Equal(t, uint64(0), cas)
				return
			}
			require.NoError(t, err)
			require.NotEqual(t, uint64(0), cas)

			xattrKeys := make([]string, 0, len(test.xattrs))
			for k := range test.xattrs {
				xattrKeys = append(xattrKeys, k)
			}
			xattrKeys = append(xattrKeys, test.xattrsToDelete...)
			doc, xattrs, getCas, err := col.GetWithXattrs(ctx, docID, xattrKeys)
			require.NoError(t, err)
			require.Equal(t, cas, getCas)
			require.JSONEq(t, string(test.body), string(doc))
			require.Equal(t, len(test.xattrs), len(xattrs), "Length of output doesn't match xattrs=%+v doesn't match input xattrs=%+v", xattrs, test.xattrs)
			for k, v := range test.xattrs {
				require.Contains(t, xattrs, k)
				require.JSONEq(t, string(v), string(xattrs[k]))
			}
		})
	}
}

func TestWriteWithXattrsSetXattrNil(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	col := bucket.DefaultDataStore()
	docID := t.Name()

	for _, fakeCas := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("cas=%d", fakeCas), func(t *testing.T) {
			_, err := col.WriteWithXattrs(ctx, docID, 0, fakeCas, []byte(`{"foo": "bar"}`), map[string][]byte{"xattr1": nil}, nil, nil)
			require.ErrorIs(t, err, sgbucket.ErrNilXattrValue)
		})
	}
}

func TestWriteTombstoneWithXattrsSetXattrNil(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	col := bucket.DefaultDataStore()
	docID := t.Name()

	for _, fakeCas := range []uint64{0, 1} {
		for _, deleteBody := range []bool{false, true} {
			t.Run(fmt.Sprintf("cas=%d, deleteBody=%v", fakeCas, deleteBody), func(t *testing.T) {
				_, err := col.WriteTombstoneWithXattrs(ctx, docID, 0, fakeCas, map[string][]byte{"_xattr1": nil}, nil, deleteBody, nil)
				require.ErrorIs(t, err, sgbucket.ErrNilXattrValue)
			})
		}
	}
}

func TestWriteWithXattrsInsertAndDeleteError(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	col := bucket.DefaultDataStore()
	docID := t.Name()

	_, err := col.WriteWithXattrs(ctx, docID, 0, 0, []byte(`{"foo": "bar"}`), map[string][]byte{"xattr1": []byte(`{"foo": "bar"}`)}, []string{"xattr2"}, nil)
	require.ErrorIs(t, err, sgbucket.ErrDeleteXattrOnDocumentInsert)
}

func requireXattrsEqual(t testing.TB, expected map[string][]byte, actual map[string][]byte) {
	require.Len(t, actual, len(expected), "Expected xattrs to be the same length %v, got %v", expected, actual)
	for k, v := range expected {
		actualV, ok := actual[k]
		if !ok {
			require.Fail(t, "Missing expected xattr %s", k)
		}
		require.JSONEq(t, string(v), string(actualV))
	}
}

func TestWriteResurrectionWithXattrs(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	col := bucket.DefaultDataStore()

	type testCase struct {
		name          string
		previousDoc   *sgbucket.BucketDocument
		updatedXattrs map[string][]byte
		updatedBody   []byte
		errorIs       error
		errorFunc     func(testing.TB, error)
	}
	tests := []testCase{
		/* this writes literal null on Couchbase server and no document on rosmar
		{
			name:      "previousDoc=nil",
			finalBody: []byte("null"),
		},
		*/
		{
			name: "previousDoc=_xattr1,xattrsToUpdate=_xattr1",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
				IsTombstone: true,
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			errorIs: sgbucket.ErrNeedBody,
		},
		{
			name: "previousDoc=_xattr1,xattrsToUpdate=_xattr1,updatedBody=nil",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			errorIs: sgbucket.ErrNeedBody,
		},
		{
			name: "previousDoc=_xattr1,xattrsToUpdate=_xattr1,updatedBody=body",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			updatedBody: []byte(`{"foo": "bar"}`),
		},
		{
			name: "previousDoc=_xattr1,xattrsToUpdate=nil,updatedBody=body",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedBody: []byte(`{"foo": "bar"}`),
		},
		// if there is no doc, WriteResurrectionWithXattrs behaves like WriteWithXattrs with cas=0
		{
			name:        "previousDoc=nil,xattrsToUpdate=nil,updatedBody=body",
			previousDoc: nil,
			updatedBody: []byte(`{"foo": "bar"}`),
		},
		{
			name: "previousDoc=alive,xattrsToUpdate=_xattr1,updatedBody=body",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
			},
			updatedXattrs: map[string][]byte{
				"_xattr1": []byte(`{"c": "d"}`),
			},
			updatedBody: []byte(`{"foo": "bar"}`),
			errorFunc:   requireDocFoundOrCasMismatchError,
		},
	}
	if bucket.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations) {
		tests = append(tests, []testCase{
			{
				name: "previousDoc=_xattr1,xattrsToUpdate=_xattr1+_xattr2,updatedBody=body",
				previousDoc: &sgbucket.BucketDocument{
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
					},
				},
				updatedXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
				},
				updatedBody: []byte(`{"foo": "bar"}`),
			},
		}...)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			docID := t.Name()
			exp := uint32(0)
			if test.previousDoc != nil {
				if test.previousDoc.Body == nil {
					_, err := col.WriteTombstoneWithXattrs(ctx, docID, exp, 0, test.previousDoc.Xattrs, nil, false, nil)
					require.NoError(t, err)
				} else {
					_, err := col.WriteWithXattrs(ctx, docID, exp, 0, test.previousDoc.Body, test.previousDoc.Xattrs, nil, nil)
					require.NoError(t, err)
				}
			}
			_, err := col.WriteResurrectionWithXattrs(ctx, docID, exp, test.updatedBody, test.updatedXattrs, nil)
			if test.errorIs != nil {
				require.ErrorIs(t, err, test.errorIs)
				return
			} else if test.errorFunc != nil {
				test.errorFunc(t, err)
				return
			}
			require.NoError(t, err)

			xattrKeys := make([]string, 0, len(test.updatedXattrs))
			for k := range test.updatedXattrs {
				xattrKeys = append(xattrKeys, k)
			}
			if test.previousDoc != nil {
				for xattrKey := range test.previousDoc.Xattrs {
					_, ok := test.updatedXattrs[xattrKey]
					if !ok {
						xattrKeys = append(xattrKeys, xattrKey)
					}
				}
			}

			body, xattrs, _, err := col.GetWithXattrs(ctx, docID, xattrKeys)
			require.NoError(t, err)
			require.JSONEq(t, string(test.updatedBody), string(body))
			requireXattrsEqual(t, test.updatedXattrs, xattrs)
		})
	}
}

func requireDocNotFoundOrCasMismatchError(t testing.TB, err error) {
	require.Error(t, err)
	if !IsDocNotFoundError(err) && !IsCasMismatch(err) {
		errMsg := fmt.Sprintf("Expected error to be either a doc not found or cas mismatch error, got %+v", err)
		require.Fail(t, errMsg)
	}
}

func requireDocFoundOrCasMismatchError(t testing.TB, err error) {
	require.Error(t, err)
	if !errors.Is(err, sgbucket.ErrKeyExists) && !IsCasMismatch(err) {
		errMsg := fmt.Sprintf("Expected error to be either a doc found or cas mismatch error, got %+v", err)
		require.Fail(t, errMsg)
	}
}
