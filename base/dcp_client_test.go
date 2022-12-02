// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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

const oneShotDCPTimeout = 5 * time.Minute

func TestOneShotDCP(t *testing.T) {

	if !TestsUseDefaultCollection() {
		// FIXME MB-53448 cannot close if high seqno is in another collection
		t.Skip("MB-53448 - One Shot DCP cannot close if high seqno is in another collection (fix in 7.2)")
	}

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	bucket := GetTestBucket(t)
	defer bucket.Close()

	dataStore := bucket.GetSingleDataStore()

	numDocs := 1000
	// write documents to bucket
	body := map[string]interface{}{"foo": "bar"}
	for i := 0; i < numDocs; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		err := dataStore.Set(key, 0, nil, body)
		require.NoError(t, err)
	}

	// create callback
	mutationCount := uint64(0)
	counterCallback := func(event sgbucket.FeedEvent) bool {
		if bytes.HasPrefix(event.Key, []byte(SyncDocPrefix)) {
			return false
		}
		atomic.AddUint64(&mutationCount, 1)
		return false
	}

	// start one shot feed
	feedID := t.Name()

	collection, err := AsCollection(dataStore)
	require.NoError(t, err)
	var collectionIDs []uint32
	if collection.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		collectionIDs = append(collectionIDs, collection.GetCollectionID())
	}

	clientOptions := DCPClientOptions{
		OneShot:       true,
		CollectionIDs: collectionIDs,
	}

	gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)
	dcpClient, err := NewDCPClient(feedID, counterCallback, clientOptions, gocbv2Bucket)
	require.NoError(t, err)

	doneChan, startErr := dcpClient.Start()
	require.NoError(t, startErr)

	defer func() {
		_ = dcpClient.Close()
	}()

	// Add additional documents in a separate goroutine, to verify one-shot behaviour
	var additionalDocsWg sync.WaitGroup
	additionalDocsWg.Add(1)
	go func() {
		defer additionalDocsWg.Done()
		updatedBody := map[string]interface{}{"foo": "bar"}
		for i := numDocs; i < numDocs*2; i++ {
			key := fmt.Sprintf("%s_INVALID_%d", t.Name(), i)
			err := dataStore.Set(key, 0, nil, updatedBody)
			require.NoError(t, err)
		}
	}()

	// deferred to block test end until we've actually finished writing docs
	defer additionalDocsWg.Wait()

	// wait for done
	timeout := time.After(oneShotDCPTimeout)
	select {
	case err := <-doneChan:
		require.NoError(t, err)
		if TestsUseDefaultCollection() {
			require.Equal(t, numDocs, int(mutationCount))
		} else {
			// CBG-2454, sometimes we get extra docs from TO_LATEST with collections
			require.LessOrEqual(t, numDocs, int(mutationCount))
		}
	case <-timeout:
		require.Fail(t, "timeout waiting for one-shot feed to complete")
	}

}

func TestTerminateDCPFeed(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	bucket := GetTestBucket(t)
	defer bucket.Close()

	dataStore := bucket.GetSingleDataStore()

	// create callback
	mutationCount := uint64(0)
	counterCallback := func(event sgbucket.FeedEvent) bool {
		atomic.AddUint64(&mutationCount, 1)
		return false
	}

	// start continuous feed with terminator
	feedID := t.Name()

	gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)
	dcpClient, err := NewDCPClient(feedID, counterCallback, DCPClientOptions{}, gocbv2Bucket)
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
			err := dataStore.Set(key, 0, nil, updatedBody)
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

	if !TestsUseDefaultCollection() {
		// FIXME MB-53448 cannot close if high seqno is in another collection
		t.Skip("MB-53448 - One Shot DCP cannot close if high seqno is in another collection (fix in 7.2)")
	}

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

			dataStore := bucket.GetSingleDataStore()

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
				err := dataStore.Set(key, 0, nil, updatedBody)
				require.NoError(t, err)
			}
			collection, ok := dataStore.(*Collection)
			require.True(t, ok)
			var collectionIDs []uint32
			if collection.IsSupported(sgbucket.BucketStoreFeatureCollections) {
				collectionIDs = append(collectionIDs, collection.GetCollectionID())
			}

			// Perform first one-shot DCP feed - normal one-shot
			dcpClientOpts := DCPClientOptions{
				OneShot:        true,
				FailOnRollback: true,
				CollectionIDs:  collectionIDs,
			}

			gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
			require.NoError(t, err)
			dcpClient, err := NewDCPClient(feedID, counterCallback, dcpClientOpts, gocbv2Bucket)
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
				CollectionIDs:   collectionIDs,
			}
			dcpClient2, err := NewDCPClient(feedID, counterCallback, dcpClientOpts, gocbv2Bucket)
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
				CollectionIDs:   collectionIDs,
			}

			dcpClient3, err := NewDCPClient(feedID, counterCallback, dcpClientOpts, gocbv2Bucket)
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

	if !TestsUseDefaultCollection() {
		// FIXME MB-53448 cannot close if high seqno is in another collection
		t.Skip("MB-53448 - One Shot DCP cannot close if high seqno is in another collection (fix in 7.2)")
	}

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	SetUpTestLogging(t, LevelDebug, KeyAll)

	bucket := GetTestBucket(t)
	defer bucket.Close()

	dataStore := bucket.GetSingleDataStore()

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
		err := dataStore.Set(key, 0, nil, updatedBody)
		require.NoError(t, err)
	}

	// Start first one-shot DCP feed, will be stopped by callback after processing 5000 records
	// Set metadata persistence frequency to zero to force persistence on every mutation
	highFrequency := 0 * time.Second

	collection, ok := dataStore.(*Collection)
	require.True(t, ok)
	var collectionIDs []uint32
	if collection.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		collectionIDs = append(collectionIDs, collection.GetCollectionID())
	}

	dcpClientOpts := DCPClientOptions{
		OneShot:                    true,
		FailOnRollback:             false,
		CheckpointPersistFrequency: &highFrequency,
		CollectionIDs:              collectionIDs,
	}

	gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)

	dcpClient, err = NewDCPClient(feedID, counterCallback, dcpClientOpts, gocbv2Bucket)
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
		CollectionIDs:  collectionIDs,
	}
	collection, ok = dataStore.(*Collection)
	require.True(t, ok)

	dcpClient2, err := NewDCPClient(feedID, secondCallback, dcpClientOpts, gocbv2Bucket)
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

	gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)

	dcpClient, err := NewDCPClient(feedID, panicCallback, dcpClientOpts, gocbv2Bucket)
	require.Error(t, err)
	require.Nil(t, dcpClient)
}
