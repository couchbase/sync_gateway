/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConflictResolver(t *testing.T) {

	defaultConflictResolverTests := []struct {
		name           string
		localDocument  Body
		remoteDocument Body
		expectedWinner Body
	}{
		{
			name:           "generation",
			localDocument:  Body{"_rev": "2-abc"},
			remoteDocument: Body{"_rev": "1-abc"},
			expectedWinner: Body{"_rev": "2-abc"},
		},
		{
			name:           "digest",
			localDocument:  Body{"_rev": "1-abc"},
			remoteDocument: Body{"_rev": "1-def"},
			expectedWinner: Body{"_rev": "1-def"},
		},
		{
			name:           "localDeleted",
			localDocument:  Body{"_rev": "2-abc", "_deleted": true},
			remoteDocument: Body{"_rev": "1-abc"},
			expectedWinner: Body{"_rev": "2-abc", "_deleted": true},
		},
		{
			name:           "remoteDeleted",
			localDocument:  Body{"_rev": "1-abc"},
			remoteDocument: Body{"_rev": "2-abc", "_deleted": true},
			expectedWinner: Body{"_rev": "2-abc", "_deleted": true},
		},
		{
			name:           "bothDeleted",
			localDocument:  Body{"_rev": "1-abc", "_deleted": true},
			remoteDocument: Body{"_rev": "2-abc", "_deleted": true},
			expectedWinner: Body{"_rev": "2-abc", "_deleted": true},
		},
	}

	for _, test := range defaultConflictResolverTests {
		t.Run(test.name, func(tt *testing.T) {
			conflict := Conflict{
				LocalDocument:  test.localDocument,
				RemoteDocument: test.remoteDocument,
			}
			result, err := DefaultConflictResolver(conflict)
			assert.NoError(t, err)
			assert.Equal(tt, test.expectedWinner, result)
		})
	}
}

func TestCustomConflictResolver(t *testing.T) {

	defaultConflictResolverTests := []struct {
		name           string
		resolverSource string
		localDocument  Body
		remoteDocument Body
		expectedWinner Body
		expectError    bool
	}{
		{
			name:           "localWins",
			resolverSource: `function(conflict) { return conflict.LocalDocument; }`,
			localDocument:  Body{"_rev": "2-abc"},
			remoteDocument: Body{"_rev": "1-abc"},
			expectedWinner: Body{"_rev": "2-abc"},
		},
		{
			name:           "remoteWins",
			resolverSource: `function(conflict) { return conflict.RemoteDocument; }`,
			localDocument:  Body{"_rev": "2-abc"},
			remoteDocument: Body{"_rev": "1-abc"},
			expectedWinner: Body{"_rev": "1-abc"},
		},
		{
			name: "merge",
			resolverSource: `function(conflict) { 
				var mergedDoc = new Object();
				mergedDoc.prop = conflict.LocalDocument.prop + conflict.RemoteDocument.prop;
				return mergedDoc;
			}`,
			localDocument:  Body{"_rev": "2-abc", "prop": "foo"},
			remoteDocument: Body{"_rev": "1-abc", "prop": "bar"},
			expectedWinner: Body{"prop": "foobar"},
		},
		{
			name: "mergeDelete",
			resolverSource: `function(conflict) { 
				return null;
			}`,
			localDocument:  Body{"_rev": "2-abc", "prop": "foo"},
			remoteDocument: Body{"_rev": "1-abc", "prop": "bar"},
			expectedWinner: Body{BodyDeleted: true},
		},
		{
			name: "invokeDefault",
			resolverSource: `function(conflict) { 
				return defaultPolicy(conflict);
			}`,
			localDocument:  Body{"_rev": "2-abc", "prop": "foo"},
			remoteDocument: Body{"_rev": "1-abc", "prop": "bar"},
			expectedWinner: Body{"_rev": "2-abc", "prop": "foo"},
		},
		{
			name: "invokeDefaultWithInvalidValue",
			resolverSource: `function(conflict) { 
				return defaultPolicy(conflict.LocalDocument);
			}`,
			localDocument:  Body{"_rev": "2-abc", "prop": "foo"},
			remoteDocument: Body{"_rev": "1-abc", "prop": "bar"},
			expectError:    true,
		},
		{
			name: "invokeDefaultWithNoValue",
			resolverSource: `function(conflict) { 
				return defaultPolicy();
			}`,
			localDocument:  Body{"_rev": "2-abc", "prop": "foo"},
			remoteDocument: Body{"_rev": "1-abc", "prop": "bar"},
			expectError:    true,
		},
		{
			name: "invokeDefaultWithNullValue",
			resolverSource: `function(conflict) { 
				return defaultPolicy(null);
			}`,
			localDocument:  Body{"_rev": "2-abc", "prop": "foo"},
			remoteDocument: Body{"_rev": "1-abc", "prop": "bar"},
			expectError:    true,
		},
	}

	for _, test := range defaultConflictResolverTests {
		t.Run(test.name, func(tt *testing.T) {
			conflict := Conflict{
				LocalDocument:  test.localDocument,
				RemoteDocument: test.remoteDocument,
			}
			customConflictResolverFunc, err := NewCustomConflictResolver(test.resolverSource)
			require.NoError(tt, err)
			result, err := customConflictResolverFunc(conflict)
			if test.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(tt, err)
			assert.Equal(tt, test.expectedWinner, result)
		})
	}
}
