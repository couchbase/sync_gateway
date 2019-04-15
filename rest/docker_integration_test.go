// +build cb_sg_dockertest

package rest

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

func TestMain(t *testing.M) {
	base.NewDockerTest(t)
}
