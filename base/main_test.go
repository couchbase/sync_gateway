package base

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	TestBucketPool = NewTestBucketPool(BucketEmptierFunc, PrimaryIndexInitFunc)

	status := m.Run()

	TestBucketPool.Close()

	os.Exit(status)
}
