// +build cb_sg_dockertest

package db

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

func TestMain(t *testing.M) {
	base.NewDockerTest(t)
}
