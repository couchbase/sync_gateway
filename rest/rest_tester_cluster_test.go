package rest

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RestTesterCluster can be used to simulate a multi-node Sync Gateway cluster.
type RestTesterCluster struct {
	testBucket      *base.TestBucket
	restTesters     []*RestTester
	roundRobinCount int64
	config          *RestTesterClusterConfig
}

// RefreshClusterDbConfigs will synchronously fetch the latest db configs from each bucket for each RestTester.
func (rtc *RestTesterCluster) RefreshClusterDbConfigs() (count int, err error) {
	for _, rt := range rtc.restTesters {
		c, err := rt.ServerContext().fetchAndLoadConfigs(false)
		if err != nil {
			return 0, err
		}
		count += c
	}
	return count, nil
}

func (rtc *RestTesterCluster) NumNodes() int {
	return len(rtc.restTesters)
}

// ForEachNode runs the given function on each RestTester node.
func (rtc *RestTesterCluster) ForEachNode(fn func(rt *RestTester)) {
	for _, rt := range rtc.restTesters {
		fn(rt)
	}
}

// RoundRobin returns the next RestTester instance, cycling through all of them sequentially.
func (rtc *RestTesterCluster) RoundRobin() *RestTester {
	requestNum := atomic.AddInt64(&rtc.roundRobinCount, 1) % int64(len(rtc.restTesters))
	node := requestNum % int64(len(rtc.restTesters))
	return rtc.restTesters[node]
}

// Node returns a specific RestTester instance.
func (rtc *RestTesterCluster) Node(i int) *RestTester {
	return rtc.restTesters[i]
}

// Close closes all of RestTester nodes and the shared TestBucket.
func (rtc *RestTesterCluster) Close() {
	for _, rt := range rtc.restTesters {
		rt.Close()
	}
	rtc.testBucket.Close()
}

// var _ base.BootstrapConnection = &testBootstrapConnection{}

type RestTesterClusterConfig struct {
	numNodes   uint8
	groupID    *string
	rtConfig   *RestTesterConfig
	testBucket *base.TestBucket
}

func defaultRestTesterClusterConfig(t *testing.T) *RestTesterClusterConfig {
	return &RestTesterClusterConfig{
		numNodes:   3,
		groupID:    base.StringPtr(t.Name()),
		rtConfig:   nil,
		testBucket: nil,
	}
}

func NewRestTesterCluster(t *testing.T, config *RestTesterClusterConfig) *RestTesterCluster {
	if base.UnitTestUrlIsWalrus() {
		// TODO: implementing a single bucket/mock base.BootstrapConnection might work here
		t.Skip("Walrus not supported for RestTesterCluster")
	}

	if config == nil {
		config = defaultRestTesterClusterConfig(t)
	}

	// Set group ID for each RestTester from cluster
	if config.rtConfig == nil {
		config.rtConfig = &RestTesterConfig{groupID: config.groupID}
	} else {
		config.rtConfig.groupID = config.groupID
	}
	// only persistent mode is supported for a RestTesterCluster
	config.rtConfig.persistentConfig = true

	// Make all RestTesters share the same unclosable TestBucket
	tb := config.testBucket
	if tb == nil {
		tb = base.GetTestBucket(t)
	}
	config.rtConfig.TestBucket = tb.NoCloseClone()

	// Start up all rest testers in parallel
	wg := sync.WaitGroup{}
	restTesters := make([]*RestTester, 0, config.numNodes)
	for i := 0; i < int(config.numNodes); i++ {
		wg.Add(1)
		go func() {
			rt := NewRestTester(t, config.rtConfig)
			// initialize the RestTester before we attempt to use it
			_ = rt.ServerContext()
			restTesters = append(restTesters, rt)
			wg.Done()
		}()
	}
	wg.Wait()

	return &RestTesterCluster{
		testBucket:  tb,
		restTesters: restTesters,
		config:      config,
	}
}

// dbConfigForTestBucket returns a barebones DbConfig for the given TestBucket.
func dbConfigForTestBucket(tb *base.TestBucket) DbConfig {
	return DbConfig{
		BucketConfig: BucketConfig{
			Bucket: base.StringPtr(tb.GetName()),
		},
		NumIndexReplicas: base.UintPtr(0),
		UseViews:         base.BoolPtr(base.TestsDisableGSI()),
		EnableXattrs:     base.BoolPtr(base.TestUseXattrs()),
	}
}

func TestPersistentDbConfigWithInvalidUpsert(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	rtc := NewRestTesterCluster(t, nil)
	defer rtc.Close()

	const db = "db"

	dbConfig := dbConfigForTestBucket(rtc.testBucket)

	// Create database on a random node.
	resp, err := rtc.RoundRobin().CreateDatabase(db, dbConfig)
	require.NoError(t, err)
	assertStatus(t, resp, http.StatusCreated)

	// A duplicate create shouldn't work, even if this were a node that doesn't have the database loaded yet.
	// But this _will_ trigger an on-demand load on this node. So now we have 2 nodes running the database.
	resp, err = rtc.RoundRobin().CreateDatabase(db, dbConfig)
	require.NoError(t, err)
	// CouchDB returns this status and body in this scenario
	assertStatus(t, resp, http.StatusPreconditionFailed)
	assert.Contains(t, string(resp.BodyBytes()), "Duplicate database name")

	// The remaining nodes will get the config via polling.
	count, err := rtc.RefreshClusterDbConfigs()
	require.NoError(t, err)
	assert.Equal(t, rtc.NumNodes()-2, count)

	// Sanity-check they have all loaded after the forced update.
	rtc.ForEachNode(func(rt *RestTester) {
		resp := rt.SendAdminRequest(http.MethodGet, "/"+db+"/", "")
		assertStatus(t, resp, http.StatusOK)
	})

	// Now we'll attempt to write an invalid database to a single node.
	// Ensure it doesn't get unloaded and is rolled back to the original database config.
	rtNode := rtc.RoundRobin()

	// upsert with an invalid config option
	resp, err = rtNode.UpsertDbConfig(db, DbConfig{RevsLimit: base.Uint32Ptr(0)})
	require.NoError(t, err)
	assertStatus(t, resp, http.StatusBadRequest)

	// On the same node, make sure the database is still running.
	resp = rtNode.SendAdminRequest(http.MethodGet, "/"+db+"/", "")
	assertStatus(t, resp, http.StatusOK)

	// and make sure we roll back the database to the previous version (without revs_limit set)
	resp = rtNode.SendAdminRequest(http.MethodGet, "/"+db+"/_config", "")
	assertStatus(t, resp, http.StatusOK)
	assert.NotContains(t, string(resp.BodyBytes()), `"revs_limit":`)

	// remove the db config directly from the bucket
	require.NoError(t, rtc.testBucket.Delete(base.PersistentConfigPrefix+*rtc.config.groupID))

	// ensure all nodes remove the database
	count, err = rtc.RefreshClusterDbConfigs()
	require.NoError(t, err)
	assert.Equal(t, 0, count)
	rtc.ForEachNode(func(rt *RestTester) {
		resp := rt.SendAdminRequest(http.MethodGet, "/"+db+"/", "")
		assertStatus(t, resp, http.StatusNotFound)
	})

}
