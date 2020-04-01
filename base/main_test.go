package base

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	TestBucketPool = NewTestBucketPool(FlushBucketEmptierFunc, NoopInitFunc)

	status := m.Run()

	TestBucketPool.Close()

	os.Exit(status)
}
