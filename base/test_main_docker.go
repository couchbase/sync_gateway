// +build cb_sg_dockertest

package base

import "testing"

func InitTestMain(t *testing.M) {
	NewDockerTest(t)
}
