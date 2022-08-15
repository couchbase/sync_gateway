package base

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const oneShotDCPTimeout = 60 * time.Second

func TestOneShotDCP(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	bucket := GetTestBucket(t)
	defer bucket.Close()

	numDocs := 1000
	// write documents to bucket
	body := map[string]interface{}{"foo": "bar"}
	for i := 0; i < numDocs; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		err := bucket.Set(key, 0, nil, body)
		require.NoError(t, err)
	}

	// create callback
	mutationCount := uint64(0)
	counterCallback := func(event sgbucket.FeedEvent) bool {
		atomic.AddUint64(&mutationCount, 1)
		return false
	}

	// start one shot feed
	feedID := t.Name()
	clientOptions := DCPClientOptions{
		OneShot: true,
	}

	collection, err := AsCollection(bucket)
	require.NoError(t, err)
	dcpClient, err := NewDCPClient(feedID, counterCallback, clientOptions, collection)
	require.NoError(t, err)

	// Add additional documents in a separate goroutine, to verify afterEndSeq handling
	var additionalDocsWg sync.WaitGroup
	additionalDocsWg.Add(1)
	go func() {
		updatedBody := map[string]interface{}{"foo": "bar"}
		for i := numDocs; i < numDocs*2; i++ {
			key := fmt.Sprintf("%s_%d", t.Name(), i)
			err := bucket.Set(key, 0, nil, updatedBody)
			assert.NoError(t, err)
		}
		additionalDocsWg.Done()
	}()

	doneChan, startErr := dcpClient.Start()
	require.NoError(t, startErr)

	defer func() {
		_ = dcpClient.Close()
	}()

	// wait for done
	timeout := time.After(oneShotDCPTimeout)
	select {
	case err := <-doneChan:
		require.Equal(t, uint64(numDocs), mutationCount)
		require.NoError(t, err)
	case <-timeout:
		t.Errorf("timeout waiting for one-shot feed to complete")
	}

	additionalDocsWg.Wait()
}

func TestTerminateDCPFeed(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	bucket := GetTestBucket(t)
	defer bucket.Close()

	// create callback
	mutationCount := uint64(0)
	counterCallback := func(event sgbucket.FeedEvent) bool {
		atomic.AddUint64(&mutationCount, 1)
		return false
	}

	// start continuous feed with terminator
	feedID := t.Name()

	collection, err := AsCollection(bucket)
	require.NoError(t, err)
	dcpClient, err := NewDCPClient(feedID, counterCallback, DCPClientOptions{}, collection)
	require.NoError(t, err)

	// Add documents in a separate goroutine
	var feedClosed AtomicBool
	var additionalDocsWg sync.WaitGroup
	additionalDocsWg.Add(1)
	go func() {
		defer additionalDocsWg.Done()
		updatedBody := map[string]interface{}{"foo": "bar"}
		for i := 0; i < 10000; i++ {
			if feedClosed.IsTrue() {
				break
			}
			key := fmt.Sprintf("%s_%d", t.Name(), i)
			err := bucket.Set(key, 0, nil, updatedBody)
			assert.NoError(t, err)
		}
	}()

	doneChan, startErr := dcpClient.Start()
	require.NoError(t, startErr)

	// Wait for some processing to complete, then close the feed
	time.Sleep(10 * time.Millisecond)
	log.Printf("Closing DCP Client")
	err = dcpClient.Close()
	log.Printf("DCP Client closed, waiting for feed close notification")
	require.NoError(t, err)

	// wait for done
	timeout := time.After(oneShotDCPTimeout)
	select {
	case <-doneChan:
		feedClosed.Set(true)
	case <-timeout:
		t.Errorf("timeout waiting for one-shot feed to complete")
	}

	log.Printf("Waiting for docs generation goroutine to exit")
	additionalDocsWg.Wait()
	log.Printf("additionalDocs wait completed")
}

// TestDCPClientMultiFeedConsistency tests for DCP rollback between execution of two DCP feeds, based on
// changes in the VbUUID
func TestDCPClientMultiFeedConsistency(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	testCases := []struct {
		startSeqNo gocbcore.SeqNo
		vbNo       uint16
		vbUUID     gocbcore.VbUUID
	}{
		{
			startSeqNo: 2,
			vbUUID:     1234, // garbage UUID
		},
		{
			startSeqNo: 0,
			vbUUID:     1234, // garbage UUID
		},
	}
	for _, test := range testCases {
		t.Run(fmt.Sprintf("metadata mismatch %+v", test), func(t *testing.T) {

			bucket := GetTestBucket(t)
			defer bucket.Close()

			// create callback
			mutationCount := uint64(0)
			vbucketZeroCount := uint64(0)
			counterCallback := func(event sgbucket.FeedEvent) bool {
				if bytes.HasPrefix(event.Key, []byte(t.Name())) {
					atomic.AddUint64(&mutationCount, 1)
					if event.VbNo == 0 {
						atomic.AddUint64(&vbucketZeroCount, 1)
					}
				}
				return false
			}

			feedID := t.Name()

			// Add documents
			updatedBody := map[string]interface{}{"foo": "bar"}
			for i := 0; i < 10000; i++ {
				key := fmt.Sprintf("%s_%d", t.Name(), i)
				err := bucket.Set(key, 0, nil, updatedBody)
				require.NoError(t, err)
			}

			// Perform first one-shot DCP feed - normal one-shot
			dcpClientOpts := DCPClientOptions{
				OneShot:        true,
				FailOnRollback: true,
			}
			collection, err := AsCollection(bucket)
			require.NoError(t, err)

			dcpClient, err := NewDCPClient(feedID, counterCallback, dcpClientOpts, collection)
			require.NoError(t, err)

			doneChan, startErr := dcpClient.Start()
			require.NoError(t, startErr)

			// Wait for first feed to complete
			feed1Timeout := time.After(oneShotDCPTimeout)
			select {
			case <-doneChan:
				mutationCount := atomic.LoadUint64(&mutationCount)
				require.Equal(t, uint64(10000), mutationCount)
			case <-feed1Timeout:
				t.Errorf("timeout waiting for first one-shot feed to complete")
			}

			// store the number of mutations from vbucket zero for validating rollback on the third feed
			vbucketZeroExpected := atomic.LoadUint64(&vbucketZeroCount)
			uuidMismatchMetadata := dcpClient.GetMetadata()

			// Perform second one-shot DCP feed - VbUUID mismatch, failOnRollback=true
			// Retrieve metadata from first DCP feed, and modify VbUUID to simulate rollback on server
			uuidMismatchMetadata[0].VbUUID = test.vbUUID
			uuidMismatchMetadata[0].StartSeqNo = test.startSeqNo

			dcpClientOpts = DCPClientOptions{
				InitialMetadata: uuidMismatchMetadata,
				FailOnRollback:  true,
				OneShot:         true,
			}
			collection, err = AsCollection(bucket)
			require.NoError(t, err)

			dcpClient2, err := NewDCPClient(feedID, counterCallback, dcpClientOpts, collection)
			require.NoError(t, err)

			doneChan2, startErr2 := dcpClient2.Start()
			require.Error(t, startErr2)

			require.NoError(t, dcpClient2.Close())
			<-doneChan2
			log.Printf("Starting third feed")
			// Perform a third DCP feed - mismatched VbUUID, failOnRollback=false
			atomic.StoreUint64(&mutationCount, 0)
			dcpClientOpts = DCPClientOptions{
				InitialMetadata: uuidMismatchMetadata,
				FailOnRollback:  false,
				OneShot:         true,
			}
			collection, err = AsCollection(bucket)
			require.NoError(t, err)

			dcpClient3, err := NewDCPClient(feedID, counterCallback, dcpClientOpts, collection)
			require.NoError(t, err)

			doneChan3, startErr3 := dcpClient3.Start()
			require.NoError(t, startErr3)

			// Wait for third feed to complete
			feed3Timeout := time.After(oneShotDCPTimeout)
			select {
			case <-doneChan3:
				// only vbucket 0 should have rolled back, expect mutation count to be only vbucketZero
				mutationCount := atomic.LoadUint64(&mutationCount)
				require.Equal(t, int(vbucketZeroExpected), int(mutationCount))
			case <-feed3Timeout:
				t.Errorf("timeout waiting for first one-shot feed to complete")
			}
		})
	}
}

// TestResumeInterruptedFeed uses persisted metadata to resume the feed
func TestResumeStoppedFeed(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	SetUpTestLogging(t, LevelDebug, KeyAll)

	bucket := GetTestBucket(t)
	defer bucket.Close()

	var dcpClient *DCPClient

	// create callback
	mutationCount := uint64(0)
	counterCallback := func(event sgbucket.FeedEvent) bool {
		if bytes.HasPrefix(event.Key, []byte(t.Name())) {
			count := atomic.AddUint64(&mutationCount, 1)
			if count > 5000 {
				err := dcpClient.Close()
				assert.NoError(t, err)
			}
		}
		return false
	}

	feedID := t.Name()

	// Add documents
	updatedBody := map[string]interface{}{"foo": "bar"}
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		err := bucket.Set(key, 0, nil, updatedBody)
		require.NoError(t, err)
	}

	// Start first one-shot DCP feed, will be stopped by callback after processing 5000 records
	// Set metadata persistence frequency to zero to force persistence on every mutation
	highFrequency := 0 * time.Second
	dcpClientOpts := DCPClientOptions{
		OneShot:                    true,
		FailOnRollback:             false,
		CheckpointPersistFrequency: &highFrequency,
	}
	collection, err := AsCollection(bucket)
	require.NoError(t, err)

	dcpClient, err = NewDCPClient(feedID, counterCallback, dcpClientOpts, collection)
	require.NoError(t, err)

	doneChan, startErr := dcpClient.Start()
	require.NoError(t, startErr)

	// Wait for first feed to complete
	timeout := time.After(oneShotDCPTimeout)
	select {
	case <-doneChan:
		mutationCount := atomic.LoadUint64(&mutationCount)
		require.Greater(t, int(mutationCount), 5000)
		log.Printf("Total processed first feed: %v", mutationCount)
	case <-timeout:
		t.Errorf("timeout waiting for first one-shot feed to complete")
	}

	var secondFeedCount uint64
	secondCallback := func(event sgbucket.FeedEvent) bool {
		if bytes.HasPrefix(event.Key, []byte(t.Name())) {
			atomic.AddUint64(&mutationCount, 1)
			atomic.AddUint64(&secondFeedCount, 1)
		}
		return false
	}

	// Perform second one-shot DCP feed with the same ID, verify it resumes and completes
	dcpClientOpts = DCPClientOptions{
		FailOnRollback: false,
		OneShot:        true,
	}
	collection, err = AsCollection(bucket)
	require.NoError(t, err)

	dcpClient2, err := NewDCPClient(feedID, secondCallback, dcpClientOpts, collection)
	require.NoError(t, err)

	doneChan2, startErr2 := dcpClient2.Start()
	require.NoError(t, startErr2)

	// Wait for second feed to complete
	timeout = time.After(oneShotDCPTimeout)
	select {
	case <-doneChan2:
		// validate the total count exceeds 10000, and the second feed didn't just reprocess everything
		mutationCount := atomic.LoadUint64(&mutationCount)
		require.GreaterOrEqual(t, int(mutationCount), 10000)
		secondFeedCount := atomic.LoadUint64(&secondFeedCount)
		require.Less(t, int(secondFeedCount), 10000)
		log.Printf("Total processed: %v, second feed: %v", mutationCount, secondFeedCount)
	case <-timeout:
		t.Errorf("timeout waiting for second one-shot feed to complete")
	}

}

// TestBadAgentPriority makes sure we can not specify agent priority as high
func TestBadAgentPriority(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server, since DCPClient requires a base.Collection")
	}

	bucket := GetTestBucket(t)
	defer bucket.Close()

	feedID := "fakeID"
	panicCallback := func(event sgbucket.FeedEvent) bool {
		t.Error(t, "Should not hit this callback")
		return false
	}
	dcpClientOpts := DCPClientOptions{
		AgentPriority: gocbcore.DcpAgentPriorityHigh,
	}
	collection, err := AsCollection(bucket)
	require.NoError(t, err)
	dcpClient, err := NewDCPClient(feedID, panicCallback, dcpClientOpts, collection)
	require.Error(t, err)
	require.Nil(t, dcpClient)
}
