package base

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/require"
)

func TestRollback(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test requires DCP feed from gocb and therefore Couchbase Sever")
	}

	c := make(chan bool)

	bucket := GetTestBucket(t)
	defer bucket.Close()
	dataStore := bucket.GetSingleDataStore()

	// create callback
	mutationCount := uint64(0)
	counterCallback := func(event sgbucket.FeedEvent) bool {
		if bytes.HasPrefix(event.Key, []byte(t.Name())) {
			atomic.AddUint64(&mutationCount, 1)
			if atomic.LoadUint64(&mutationCount) == uint64(1000) {
				c <- true
			}
		}
		return false
	}

	feedID := t.Name()
	gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)

	dcpClientOpts := DCPClientOptions{
		FailOnRollback:    false,
		OneShot:           false,
		CollectionIDs:     getCollectionIDs(t, bucket),
		CheckpointPrefix:  DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
		MetadataStoreType: DCPMetadataStoreInMemory,
	}

	// timeout for feed to complete
	timeout := time.After(20 * time.Second)

	dcpClient, err := NewDCPClient(feedID, counterCallback, dcpClientOpts, gocbv2Bucket)
	require.NoError(t, err)

	doneChan, startErr := dcpClient.Start()
	require.NoError(t, startErr)

	// Add documents
	const numDocs = 1000
	updatedBody := map[string]interface{}{"foo": "bar"}
	for i := 0; i < numDocs; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		err := dataStore.Set(key, 0, nil, updatedBody)
		require.NoError(t, err)
	}

	// wait for a timeout to ensure client streams all mutations over continuous feed
	select {
	case <-c:
		mutationCount := atomic.LoadUint64(&mutationCount)
		require.Equal(t, uint64(1000), mutationCount)
	case <-timeout:
		t.Fatalf("timeout on client reached")
	}

	require.NoError(t, dcpClient.Close())
	<-doneChan

	metadata := dcpClient.GetMetadata()
	fmt.Println("schedule rollover here")
	cmd := exec.Command("/bin/bash", "/home/tcolvin/repos/sync_gateway/failover.sh")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	t.Logf("running %s", cmd)
	require.NoError(t, cmd.Run())
	dcpClientOpts = DCPClientOptions{
		InitialMetadata:   metadata,
		FailOnRollback:    false,
		OneShot:           false,
		CollectionIDs:     getCollectionIDs(t, bucket),
		CheckpointPrefix:  DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
		MetadataStoreType: DCPMetadataStoreInMemory,
	}

	// timeout for feed to complete
	timeout = time.After(20 * time.Second)
	t.Logf("starting new dcp client")

	dcpClient, err = NewDCPClient(feedID, counterCallback, dcpClientOpts, gocbv2Bucket)
	require.NoError(t, err)

	_, startErr = dcpClient.Start()
	require.NoError(t, startErr)
	t.Logf("finished new dcp client")

	mutationCount = uint64(0)
	// Add documents
	for i := 0; i < numDocs; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i+numDocs)
		err := dataStore.Set(key, 0, nil, updatedBody)
		require.NoError(t, err)
	}

	// wait for a timeout to ensure client streams all mutations over continuous feed
	select {
	case <-c:
		mutationCount := atomic.LoadUint64(&mutationCount)
		require.Equal(t, uint64(1000), mutationCount)
	case <-timeout:
		t.Fatalf("timeout on client reached")
	}
}
