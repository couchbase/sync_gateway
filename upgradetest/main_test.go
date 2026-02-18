package upgradetest

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	runTests, _ := strconv.ParseBool(os.Getenv(base.TbpEnvUpgradeTests))
	if !runTests {
		base.SkipTestMain(m, "Upgrade tests are disabled, to enable set %s=true environment variable", base.TbpEnvUpgradeTests)
		return
	}

	globalDockertestPool = newDockertestPool(m)

	// Do not create indexes for this test, so they are built by each container during SG database startup. Much slower but allows proper versioned testing.
	tbpOptions := base.TestBucketPoolOptions{MemWatermarkThresholdMB: 8192, NumCollectionsPerBucket: 1}
	db.TestBucketPoolEnsureNoIndexes(ctx, m, tbpOptions)
}
