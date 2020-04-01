package db

import (
	"os"
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

func TestMain(m *testing.M) {
	base.TestBucketPool = base.NewTestBucketPool(ViewsAndGSIBucketReadier, ViewsAndGSIBucketInit)

	status := m.Run()

	base.TestBucketPool.Close()

	os.Exit(status)
}
