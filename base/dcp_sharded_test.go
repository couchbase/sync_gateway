package base

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexName(t *testing.T) {
	tests := []struct {
		dbName    string
		indexName string
	}{
		{
			dbName:    "",
			indexName: "db0x0_index",
		},
		{
			dbName:    "foo",
			indexName: "db0xcfc4ae1d_index",
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("dbName %s -> indexName %s", test.indexName, test.dbName), func(t *testing.T) {
			require.Equal(t, test.indexName, GenerateIndexName(test.dbName))
		})
	}
}
