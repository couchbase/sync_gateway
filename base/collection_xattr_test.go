// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/require"
)

func TestWriteTombstoneWithXattrs(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("CBG-3895 will match behavior with rosmar")
	}
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
			writeErrorFunc: RequireDocNotFoundError,
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
			writeErrorFunc: RequireDocNotFoundError,
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
			writeErrorFunc: RequireDocNotFoundError,
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
			cas:        fakeCas,
			finalBody:  []byte("{}"), // this seems wrong
			deleteBody: false,
			finalXattrs: map[string][]byte{
				"_xattr1": []byte(`{"a" : "b"}`),
			},
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
				cas:        fakeCas,
				deleteBody: false,
				// this seems wrong, no error??
				finalBody: []byte("{}"),
				finalXattrs: map[string][]byte{
					"_xattr1": []byte(`{"c": "d"}`),
					"_xattr2": []byte(`{"f": "g"}`),
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
					casOut, err = col.WriteTombstoneWithXattrs(ctx, docID, exp, 0, test.previousDoc.Xattrs, false, nil)
				} else {
					casOut, err = col.WriteWithXattrs(ctx, docID, exp, 0, test.previousDoc.Body, test.previousDoc.Xattrs, nil)
				}
				require.NoError(t, err)
				if test.cas == previousCas {
					cas = casOut
				}
			}
			_, err := col.WriteTombstoneWithXattrs(ctx, docID, exp, cas, test.updatedXattrs, test.deleteBody, nil)
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
			if test.finalBody == nil {
				require.Equal(t, "", string(body))
			} else {
				require.JSONEq(t, string(test.finalBody), string(body))
			}
			requireXattrsEqual(t, test.finalXattrs, xattrs)
		})
	}
}

func TestWriteUpdateWithXattrs(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("CBG-3895 will match behavior with rosmar")
	}
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
		{
			name:      "previousDoc=nil,updatedDoc=nil",
			finalBody: []byte("null"),
		},
		{
			name: "previousDoc=body+_xattr1,updatedDoc=nil",
			previousDoc: &sgbucket.BucketDocument{
				Body: []byte(`{"foo": "bar"}`),
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			errorFunc: requireMutateInOneOpError,
		},
		{
			name: "previousDoc=_xattr1,updatedDoc=nil",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			errorFunc: requireMutateInOneOpError,
		},
		{
			name:        "previousDoc=nil,updatedDoc=_xattr1",
			updatedDoc:  sgbucket.UpdatedDoc{Xattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)}},
			finalBody:   []byte("null"),
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
			updatedDoc:  sgbucket.UpdatedDoc{Xattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)}},
			finalBody:   []byte(`{"foo": "bar"}`),
			finalXattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
		},
		{
			name: "previousDoc=_xattr1,updatedDoc=_xattr1",
			previousDoc: &sgbucket.BucketDocument{
				Xattrs: map[string][]byte{
					"_xattr1": []byte(`{"a" : "b"}`),
				},
			},
			updatedDoc:  sgbucket.UpdatedDoc{Xattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)}},
			finalXattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)},
		},
	}

	if bucket.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations) {
		tests = append(tests, []testCase{
			{
				name: "previousDoc=body+_xattr1,xattr2,updatedDoc=_xattr1",
				previousDoc: &sgbucket.BucketDocument{
					Body: []byte(`{"foo": "bar"}`),
					Xattrs: map[string][]byte{
						"_xattr1": []byte(`{"a" : "b"}`),
						"_xattr2": []byte(`{"e" : "f"}`),
					},
				},
				updatedDoc: sgbucket.UpdatedDoc{Xattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)}},
				finalBody:  []byte(`{"foo": "bar"}`),
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
				updatedDoc: sgbucket.UpdatedDoc{Xattrs: map[string][]byte{"_xattr1": []byte(`{"c" : "d"}`)}},
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
					_, err := col.WriteTombstoneWithXattrs(ctx, docID, exp, cas, test.previousDoc.Xattrs, deleteBody, mutateInOptions)
					require.NoError(t, err)
				} else {
					_, err := col.WriteWithXattrs(ctx, docID, exp, cas, test.previousDoc.Body, test.previousDoc.Xattrs, mutateInOptions)
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
			if len(test.finalBody) > 0 {
				require.JSONEq(t, string(test.finalBody), string(body))
			} else {
				require.Equal(t, string(test.finalBody), string(body))
			}
			requireXattrsEqual(t, test.finalXattrs, xattrs)
		})
	}
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

func requireMutateInOneOpError(t testing.TB, err error) {
	require.ErrorContains(t, err, "at least one op must be present: invalid argument")
}
