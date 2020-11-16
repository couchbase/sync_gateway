package channels

import (
	"os"
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

func TestMain(m *testing.M) {
	defer base.SetUpGlobalTestLogging(m)()

	status := m.Run()

	os.Exit(status)
}
