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
			expectedWinner: Body{"_rev": "1-abc"},
		},
		{
			name:           "remoteDeleted",
			localDocument:  Body{"_rev": "1-abc"},
			remoteDocument: Body{"_rev": "2-abc", "_deleted": true},
			expectedWinner: Body{"_rev": "1-abc"},
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
			assert.NoError(tt, err)
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
			assert.NoError(tt, err)
			assert.Equal(tt, test.expectedWinner, result)
		})
	}
}
