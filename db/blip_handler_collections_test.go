package db

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

func TestParseScopeCollection(t *testing.T) {
	testCases := []struct {
		collectionString string
		scope            *string
		collection       *string
		err              bool
	}{
		{
			collectionString: "foo.bar",
			scope:            base.StringPtr("foo"),
			collection:       base.StringPtr("bar"),
			err:              false,
		},
		{
			collectionString: "foo",
			scope:            base.StringPtr(base.DefaultScope),
			collection:       base.StringPtr("foo"),
			err:              false,
		},
		{
			collectionString: "",
			scope:            nil,
			collection:       nil,
			err:              true,
		},
		{
			collectionString: ".",
			scope:            nil,
			collection:       nil,
			err:              true,
		},
		{
			collectionString: ".bar",
			scope:            nil,
			collection:       nil,
			err:              true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.collectionString, func(t *testing.T) {
			scope, collection, err := parseScopeAndCollection(testCase.collectionString)
			if testCase.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, testCase.scope, scope)
			require.Equal(t, testCase.collection, collection)

		})
	}
}

func TestIsDefaultCollection(t *testing.T) {
	testCases := []struct {
		scope      string
		collection string
		isDefault  bool
	}{
		{
			scope:      "foo",
			collection: "bar",
			isDefault:  false,
		},
		{
			scope:      "_default",
			collection: "bar",
			isDefault:  false,
		},

		{
			scope:      "_default",
			collection: "_default",
			isDefault:  true,
		},
	}
	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s.%s", testCase.scope, testCase.collection), func(t *testing.T) {
			require.Equal(t, testCase.isDefault, isDefaultScopeAndCollection(testCase.scope, testCase.collection))
		})
	}
}
