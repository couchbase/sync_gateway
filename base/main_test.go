package base

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	defer SetUpGlobalTestLogging(m)()

	GTestBucketPool = NewTestBucketPool(FlushBucketEmptierFunc, NoopInitFunc)

	status := m.Run()

	GTestBucketPool.Close()

	os.Exit(status)
}
