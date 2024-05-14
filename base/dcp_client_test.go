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

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

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
		OneShot:          true,
		CollectionIDs:    collectionIDs,
		CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
	}

	gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)
	dcpClient, err := NewDCPClient(TestCtx(t), feedID, counterCallback, clientOptions, gocbv2Bucket)
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
		require.LessOrEqual(t, numDocs, int(mutationCount))
	case <-timeout:
		t.Errorf("timeout waiting for one-shot feed to complete")
	}

}

func TestTerminateDCPFeed(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

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
	options := DCPClientOptions{
		CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
	}
	dcpClient, err := NewDCPClient(TestCtx(t), feedID, counterCallback, options, gocbv2Bucket)
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
		t.Run(fmt.Sprintf("metadata mismatch start at %d", test.startSeqNo), func(t *testing.T) {

			ctx := TestCtx(t)
			bucket := GetTestBucket(t)
			defer bucket.Close(ctx)

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
				OneShot:          true,
				FailOnRollback:   true,
				CollectionIDs:    collectionIDs,
				CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
			}

			gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
			require.NoError(t, err)
			dcpClient, err := NewDCPClient(ctx, feedID, counterCallback, dcpClientOpts, gocbv2Bucket)
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
			uuidMismatchMetadata[0].SnapStartSeqNo = test.startSeqNo
			uuidMismatchMetadata[0].SnapEndSeqNo = test.startSeqNo

			dcpClientOpts = DCPClientOptions{
				InitialMetadata:  uuidMismatchMetadata,
				FailOnRollback:   true,
				OneShot:          true,
				CollectionIDs:    collectionIDs,
				CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
			}
			dcpClient2, err := NewDCPClient(ctx, feedID, counterCallback, dcpClientOpts, gocbv2Bucket)
			require.NoError(t, err)

			doneChan2, startErr2 := dcpClient2.Start()
			require.Error(t, startErr2)

			require.NoError(t, dcpClient2.Close())
			<-doneChan2
			log.Printf("Starting third feed")
			// Perform a third DCP feed - mismatched VbUUID, failOnRollback=false
			atomic.StoreUint64(&mutationCount, 0)
			dcpClientOpts = DCPClientOptions{
				InitialMetadata:  uuidMismatchMetadata,
				FailOnRollback:   false,
				OneShot:          true,
				CollectionIDs:    collectionIDs,
				CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
			}

			dcpClient3, err := NewDCPClient(ctx, feedID, counterCallback, dcpClientOpts, gocbv2Bucket)
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
				// check the rolled back vBucket has in fact closed the stream after its finished
				numVBuckets := len(dcpClient.activeVbuckets)
				require.Equal(t, uint16(0), uint16(numVBuckets))
			case <-feed3Timeout:
				t.Errorf("timeout waiting for first one-shot feed to complete")
			}
		})
	}
}

func TestContinuousDCPRollback(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test requires DCP feed from gocb and therefore Couchbase Sever")
	}

	var vbUUID gocbcore.VbUUID = 1234
	c := make(chan bool)

	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	dataStore := bucket.GetSingleDataStore()

	// create callback
	mutationCount := uint64(0)
	counterCallback := func(event sgbucket.FeedEvent) bool {
		if bytes.HasPrefix(event.Key, []byte(t.Name())) {
			atomic.AddUint64(&mutationCount, 1)
			if atomic.LoadUint64(&mutationCount) == uint64(10000) {
				c <- true
			}
		}
		return false
	}

	feedID := t.Name()
	gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)

	collection, err := AsCollection(dataStore)
	require.NoError(t, err)

	var collectionIDs []uint32
	if collection.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		collectionIDs = append(collectionIDs, collection.GetCollectionID())
	}

	dcpClientOpts := DCPClientOptions{
		FailOnRollback:    false,
		OneShot:           false,
		CollectionIDs:     collectionIDs,
		CheckpointPrefix:  DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
		MetadataStoreType: DCPMetadataStoreInMemory,
	}

	// timeout for feed to complete
	timeout := time.After(20 * time.Second)

	dcpClient, err := NewDCPClient(ctx, feedID, counterCallback, dcpClientOpts, gocbv2Bucket)
	require.NoError(t, err)

	_, startErr := dcpClient.Start()
	require.NoError(t, startErr)

	// Add documents
	const numDocs = 10000
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
		require.Equal(t, uint64(10000), mutationCount)
	case <-timeout:
		t.Fatalf("timeout on client reached")
	}

	// new dcp client to simulate a rollback
	dcpClientOpts = DCPClientOptions{
		InitialMetadata:   dcpClient.GetMetadata(),
		FailOnRollback:    false,
		OneShot:           false,
		CollectionIDs:     collectionIDs,
		CheckpointPrefix:  DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
		MetadataStoreType: DCPMetadataStoreInMemory,
	}
	require.NoError(t, dcpClient.Close())

	dcpClient1, err := NewDCPClient(ctx, feedID, counterCallback, dcpClientOpts, gocbv2Bucket)
	require.NoError(t, err)
	// function to force the rollback of some vBuckets
	dcpClient1.forceRollbackvBucket(vbUUID)

	_, startErr = dcpClient1.Start()
	require.NoError(t, startErr)

	// Assert that the number of vBuckets active are the same as the total number of vBuckets on the client.
	// In continuous rollback the streams should not close after they're finished.
	numVBuckets := len(dcpClient1.activeVbuckets)
	require.Equal(t, dcpClient1.numVbuckets, uint16(numVBuckets))

	defer func() {
		assert.NoError(t, dcpClient1.Close())
	}()

}

// forceRollbackvBucket forces the rollback of vBucket IDs that are even
// Test helper function. This should not be used elsewhere.
func (dc *DCPClient) forceRollbackvBucket(uuid gocbcore.VbUUID) {
	metadata := make([]DCPMetadata, dc.numVbuckets)
	for i := uint16(0); i < dc.numVbuckets; i++ {
		// rollback roughly half the vBuckets
		if i%2 == 0 {
			metadata[i] = dc.metadata.GetMeta(i)
			metadata[i].VbUUID = uuid
			dc.metadata.SetMeta(i, metadata[i])
		}
	}
}

// TestResumeInterruptedFeed uses persisted metadata to resume the feed
func TestResumeStoppedFeed(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	SetUpTestLogging(t, LevelDebug, KeyAll)

	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

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
		CheckpointPrefix:           DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
	}

	gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)

	dcpClient, err = NewDCPClient(ctx, feedID, counterCallback, dcpClientOpts, gocbv2Bucket)
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
		FailOnRollback:   false,
		OneShot:          true,
		CollectionIDs:    collectionIDs,
		CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
	}

	dcpClient2, err := NewDCPClient(ctx, feedID, secondCallback, dcpClientOpts, gocbv2Bucket)
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

	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

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

	dcpClient, err := NewDCPClient(TestCtx(t), feedID, panicCallback, dcpClientOpts, gocbv2Bucket)
	require.Error(t, err)
	require.Nil(t, dcpClient)
}

func TestDCPOutOfRangeSequence(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test requires DCP feed from gocb and therefore Couchbase Sever")
	}

	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	// create callback
	callback := func(event sgbucket.FeedEvent) bool {
		t.Fatalf("Unexpected callback: %+v", event)
		return false
	}

	feedID := t.Name()
	dcpClientOpts := DCPClientOptions{
		FailOnRollback:    false,
		OneShot:           true,
		CollectionIDs:     getCollectionIDs(t, bucket),
		CheckpointPrefix:  DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
		MetadataStoreType: DCPMetadataStoreInMemory,
	}

	// timeout for feed to complete
	timeout := time.After(20 * time.Second)

	gocbv2Bucket, err := AsGocbV2Bucket(bucket)
	require.NoError(t, err)

	dcpClient, err := NewDCPClient(ctx, feedID, callback, dcpClientOpts, gocbv2Bucket)
	require.NoError(t, err)

	doneChan, startErr := dcpClient.Start()
	require.NoError(t, startErr)
	defer func() {
		assert.NoError(t, dcpClient.Close())
	}()

	select {
	case <-doneChan:
		break
	case <-timeout:
		t.Fatalf("timeout on client reached")
	}

	metadata := dcpClient.GetMetadata()
	metadata[1].StartSeqNo = 1000 // out of range
	dcpClientOpts = DCPClientOptions{
		FailOnRollback:    false,
		OneShot:           true,
		CollectionIDs:     getCollectionIDs(t, bucket),
		CheckpointPrefix:  DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
		MetadataStoreType: DCPMetadataStoreInMemory,
		InitialMetadata:   metadata,
	}

	dcpClient, err = NewDCPClient(ctx, feedID, callback, dcpClientOpts, gocbv2Bucket)
	require.NoError(t, err)

	_, startErr = dcpClient.Start()
	require.Error(t, startErr)
	require.Contains(t, startErr.Error(), "out of range")

}

func getCollectionIDs(t *testing.T, bucket *TestBucket) []uint32 {
	collection, err := AsCollection(bucket.GetSingleDataStore())
	require.NoError(t, err)

	var collectionIDs []uint32
	if collection.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		collectionIDs = append(collectionIDs, collection.GetCollectionID())
	}
	return collectionIDs

}

func TestDCPFeedEventTypes(t *testing.T) {
	TestRequiresGocbDCPClient(t)

	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	collection := bucket.GetSingleDataStore()

	// start one shot feed
	var collectionIDs []uint32
	if collection.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		collectionIDs = append(collectionIDs, collection.GetCollectionID())
	}

	clientOptions := DCPClientOptions{
		CollectionIDs:    collectionIDs,
		CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
	}

	gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)

	testOver := make(chan struct{})
	docID := t.Name()
	var dcpMutationCas uint64
	var dcpMutationRevNo uint64
	var dcpDeletionCas uint64
	var dcpDeletionRevNo uint64
	// create callback
	callback := func(event sgbucket.FeedEvent) bool {
		fmt.Printf("!! event: %+v\n", event)
		// other doc events can happen from previous tests
		if docID != string(event.Key) {
			return true
		}
		switch event.Opcode {
		case sgbucket.FeedOpMutation:
			dcpMutationCas = event.Cas
			dcpMutationRevNo = event.RevNo
			require.NotEqual(t, uint64(0), dcpMutationCas)
			require.NotEqual(t, uint64(0), dcpMutationRevNo)
		case sgbucket.FeedOpDeletion:
			defer close(testOver)

			dcpDeletionCas = event.Cas
			dcpDeletionRevNo = event.RevNo
			// FIXME: I am surprised these values are zero
			require.NotEqual(t, uint64(0), dcpDeletionCas)
			require.NotEqual(t, uint64(0), dcpDeletionRevNo)
		}
		return true
	}

	dcpClient, err := NewDCPClient(ctx, t.Name(), callback, clientOptions, gocbv2Bucket)
	require.NoError(t, err)

	doneChan, startErr := dcpClient.Start()
	require.NoError(t, startErr)

	defer func() {
		_ = dcpClient.Close() // extra close in case of early exit
	}()
	xattrName := "_xattr1"
	xattrBody := []byte(`{"an": "xattr"}`)
	writeMutationCas, err := collection.WriteWithXattrs(ctx, docID, 0, 0, []byte(`{"foo":"bar"}`), map[string][]byte{xattrName: xattrBody}, nil)
	require.NoError(t, err)

	deleteMutationCas, err := collection.Remove(docID, writeMutationCas)
	require.NoError(t, err)

	timeout := time.After(time.Second * 5)
	select {
	case <-testOver:
		require.NoError(t, dcpClient.Close())
	case <-timeout:
		t.Fatalf("timeout waiting for doc write/deletion to complete")
	}
	require.NoError(t, <-doneChan)

	xattrs, _, err := collection.GetXattrs(ctx, docID, []string{"_xattr1"})
	require.NoError(t, err)
	require.JSONEq(t, string(xattrBody), string(xattrs[xattrName]))

	require.Equal(t, writeMutationCas, dcpMutationCas)
	require.Equal(t, deleteMutationCas, dcpDeletionCas)
	require.NotEqual(t, dcpMutationRevNo, dcpDeletionRevNo)

}
