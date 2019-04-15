// +build !cb_sg_dockertest

package base

import (
	"os"
	"testing"
)

func InitTestMain(t *testing.M) {
	os.Exit(t.Run())
}
