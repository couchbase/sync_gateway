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

	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	dataStore := bucket.GetSingleDataStore()

	numDocs := 1000
	// write documents to bucket
	body := map[string]any{"foo": "bar"}
	for i := range numDocs {
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

	dcpOptions := DCPClientOptions{
		CollectionNames:  NewCollectionNameSet(dataStore),
		OneShot:          true,
		CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
		Callback:         counterCallback,
	}

	dcpClient, err := NewDCPClient(ctx, bucket, dcpOptions)
	require.NoError(t, err)

	doneChan, startErr := dcpClient.Start()
	require.NoError(t, startErr)

	defer func() {
		_ = dcpClient.Close()
		RequireChanClosed(t, doneChan)
	}()

	// Add additional documents in a separate goroutine, to verify one-shot behaviour
	var additionalDocsWg sync.WaitGroup
	additionalDocsWg.Add(1)
	go func() {
		defer additionalDocsWg.Done()
		updatedBody := map[string]any{"foo": "bar"}
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

	dcpOptions := DCPClientOptions{
		CollectionNames:  NewCollectionNameSet(dataStore),
		OneShot:          false,
		CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
		Callback:         counterCallback,
	}

	dcpClient, err := NewDCPClient(ctx, bucket, dcpOptions)
	require.NoError(t, err)

	// Add documents in a separate goroutine
	var feedClosed AtomicBool
	var additionalDocsWg sync.WaitGroup
	additionalDocsWg.Add(1)
	go func() {
		defer additionalDocsWg.Done()
		updatedBody := map[string]any{"foo": "bar"}
		for i := range 10000 {
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

			// Add documents
			updatedBody := map[string]any{"foo": "bar"}
			for i := range 10000 {
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
			dcpClientOpts := GoCBDCPClientOptions{
				OneShot:          true,
				FailOnRollback:   true,
				CollectionIDs:    collectionIDs,
				CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
			}

			gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
			require.NoError(t, err)
			dcpClient, err := NewGocbDCPClient(ctx, counterCallback, dcpClientOpts, gocbv2Bucket)
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

			dcpClientOpts = GoCBDCPClientOptions{
				InitialMetadata:  uuidMismatchMetadata,
				FailOnRollback:   true,
				OneShot:          true,
				CollectionIDs:    collectionIDs,
				CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
			}
			dcpClient2, err := NewGocbDCPClient(ctx, counterCallback, dcpClientOpts, gocbv2Bucket)
			require.NoError(t, err)

			doneChan2, startErr2 := dcpClient2.Start()
			require.Error(t, startErr2)

			require.NoError(t, dcpClient2.Close())
			<-doneChan2
			log.Printf("Starting third feed")
			// Perform a third DCP feed - mismatched VbUUID, failOnRollback=false
			atomic.StoreUint64(&mutationCount, 0)
			dcpClientOpts = GoCBDCPClientOptions{
				InitialMetadata:  uuidMismatchMetadata,
				FailOnRollback:   false,
				OneShot:          true,
				CollectionIDs:    collectionIDs,
				CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
			}

			dcpClient3, err := NewGocbDCPClient(ctx, counterCallback, dcpClientOpts, gocbv2Bucket)
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

	gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)

	collection, err := AsCollection(dataStore)
	require.NoError(t, err)

	var collectionIDs []uint32
	if collection.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		collectionIDs = append(collectionIDs, collection.GetCollectionID())
	}

	dcpClientOpts := GoCBDCPClientOptions{
		FailOnRollback:    false,
		OneShot:           false,
		CollectionIDs:     collectionIDs,
		CheckpointPrefix:  DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
		MetadataStoreType: DCPMetadataStoreInMemory,
	}

	// timeout for feed to complete
	timeout := time.After(20 * time.Second)

	dcpClient, err := NewGocbDCPClient(ctx, counterCallback, dcpClientOpts, gocbv2Bucket)
	require.NoError(t, err)

	_, startErr := dcpClient.Start()
	require.NoError(t, startErr)

	// Add documents
	const numDocs = 10000
	updatedBody := map[string]any{"foo": "bar"}
	for i := range numDocs {
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
	dcpClientOpts = GoCBDCPClientOptions{
		InitialMetadata:   dcpClient.GetMetadata(),
		FailOnRollback:    false,
		OneShot:           false,
		CollectionIDs:     collectionIDs,
		CheckpointPrefix:  DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
		MetadataStoreType: DCPMetadataStoreInMemory,
	}
	require.NoError(t, dcpClient.Close())

	dcpClient1, err := NewGocbDCPClient(ctx, counterCallback, dcpClientOpts, gocbv2Bucket)
	require.NoError(t, err)
	// function to force the rollback of some vBuckets
	dcpClient1.forceRollbackvBucket(vbUUID)

	doneChan, startErr := dcpClient1.Start()
	require.NoError(t, startErr)

	defer func() {
		assert.NoError(t, dcpClient1.Close())
		RequireChanClosed(t, doneChan)
	}()

	// Assert that the number of vBuckets active are the same as the total number of vBuckets on the client.
	// In continuous rollback the streams should not close after they're finished.
	numVBuckets := len(dcpClient1.activeVbuckets)
	require.Equal(t, dcpClient1.numVbuckets, uint16(numVBuckets))

}

// forceRollbackvBucket forces the rollback of vBucket IDs that are even
// Test helper function. This should not be used elsewhere.
func (dc *GoCBDCPClient) forceRollbackvBucket(uuid gocbcore.VbUUID) {
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
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	dataStore := bucket.GetSingleDataStore()

	var dcpClient DCPClient

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

	// Add documents
	updatedBody := map[string]any{"foo": "bar"}
	for i := range 10000 {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		err := dataStore.Set(key, 0, nil, updatedBody)
		require.NoError(t, err)
	}

	dcpClientOpts := DCPClientOptions{
		Callback:         counterCallback,
		CollectionNames:  NewCollectionNameSet(dataStore),
		OneShot:          true,
		FailOnRollback:   false,
		CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
	}

	dcpClient, err := NewDCPClient(ctx, bucket, dcpClientOpts)
	require.NoError(t, err)

	if !UnitTestUrlIsWalrus() {
		dc, ok := dcpClient.(*GoCBDCPClient)
		require.True(t, ok)
		dc.checkpointPersistFrequency = Ptr(0 * time.Second) // disable periodic checkpointing for test
	}

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
		Callback:         secondCallback,
		CollectionNames:  NewCollectionNameSet(dataStore),
		CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
	}

	dcpClient2, err := NewDCPClient(ctx, bucket, dcpClientOpts)
	require.NoError(t, err)

	if !UnitTestUrlIsWalrus() {
		dc, ok := dcpClient2.(*GoCBDCPClient)
		require.True(t, ok)
		dc.checkpointPersistFrequency = Ptr(0 * time.Second) // disable periodic checkpointing for test
	}

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

	panicCallback := func(event sgbucket.FeedEvent) bool {
		t.Error(t, "Should not hit this callback")
		return false
	}
	dcpClientOpts := GoCBDCPClientOptions{
		AgentPriority: gocbcore.DcpAgentPriorityHigh,
	}

	gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)

	dcpClient, err := NewGocbDCPClient(TestCtx(t), panicCallback, dcpClientOpts, gocbv2Bucket)
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

	dcpClientOpts := GoCBDCPClientOptions{
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

	dcpClient, err := NewGocbDCPClient(ctx, callback, dcpClientOpts, gocbv2Bucket)
	require.NoError(t, err)

	doneChan, startErr := dcpClient.Start()
	require.NoError(t, startErr)
	defer func() {
		assert.NoError(t, dcpClient.Close())
		RequireChanClosed(t, doneChan)
	}()

	select {
	case <-doneChan:
		break
	case <-timeout:
		t.Fatalf("timeout on client reached")
	}

	metadata := dcpClient.GetMetadata()
	metadata[1].StartSeqNo = 1000 // out of range
	dcpClientOpts = GoCBDCPClientOptions{
		FailOnRollback:    false,
		OneShot:           true,
		CollectionIDs:     getCollectionIDs(t, bucket),
		CheckpointPrefix:  DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
		MetadataStoreType: DCPMetadataStoreInMemory,
		InitialMetadata:   metadata,
	}

	dcpClient, err = NewGocbDCPClient(ctx, callback, dcpClientOpts, gocbv2Bucket)
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
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	collection := bucket.GetSingleDataStore()

	foundEvent := make(chan struct{})
	docID := t.Name()
	var dcpMutationCas uint64
	var dcpMutationRevNo uint64
	var dcpDeletionCas uint64
	var dcpDeletionRevNo uint64
	// create callback
	callback := func(event sgbucket.FeedEvent) bool {
		// other doc events can happen from previous tests
		if docID != string(event.Key) {
			return true
		}
		switch event.Opcode {
		case sgbucket.FeedOpMutation:
			defer func() {
				foundEvent <- struct{}{}
			}()
			dcpMutationCas = event.Cas
			dcpMutationRevNo = event.RevNo
			require.NotEqual(t, uint64(0), dcpMutationCas)
			require.NotEqual(t, uint64(0), dcpMutationRevNo)
		case sgbucket.FeedOpDeletion:
			defer close(foundEvent)

			dcpDeletionCas = event.Cas
			dcpDeletionRevNo = event.RevNo
			require.NotEqual(t, uint64(0), dcpDeletionCas)
			require.NotEqual(t, uint64(0), dcpDeletionRevNo)
		}
		return true
	}
	clientOptions := DCPClientOptions{
		CollectionNames:  NewCollectionNameSet(collection),
		CheckpointPrefix: DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
		Callback:         callback,
	}
	dcpClient, err := NewDCPClient(ctx, bucket, clientOptions)
	require.NoError(t, err)

	doneChan, startErr := dcpClient.Start()
	require.NoError(t, startErr)

	defer func() {
		_ = dcpClient.Close() // extra close in case of early exit
		RequireChanClosed(t, doneChan)
	}()
	xattrName := "_xattr1"
	xattrBody := []byte(`{"an": "xattr"}`)
	writeMutationCas, err := collection.WriteWithXattrs(ctx, docID, 0, 0, []byte(`{"foo":"bar"}`), map[string][]byte{xattrName: xattrBody}, nil, nil)
	require.NoError(t, err)

	// make sure mutation is processed
	timeout := time.After(time.Second * 5)
	select {
	case <-foundEvent:
	case <-timeout:
		t.Fatalf("timeout waiting for doc mutation")
	}

	deleteMutationCas, err := collection.Remove(docID, writeMutationCas)
	require.NoError(t, err)

	select {
	case <-foundEvent:
		require.NoError(t, dcpClient.Close())
	case <-timeout:
		t.Fatalf("timeout waiting for doc deletion")
	}
	RequireChanClosed(t, doneChan)

	xattrs, _, err := collection.GetXattrs(ctx, docID, []string{"_xattr1"})
	require.NoError(t, err)
	require.JSONEq(t, string(xattrBody), string(xattrs[xattrName]))

	require.Equal(t, writeMutationCas, dcpMutationCas)
	require.Equal(t, deleteMutationCas, dcpDeletionCas)
	require.NotEqual(t, dcpMutationRevNo, dcpDeletionRevNo)

}

// TestDCPFeedContentBodyOnlyDocs verifies that body-only documents (no xattrs) are delivered
// on DCP feeds using different FeedContent modes. This specifically targets the
// NoValueWithUnderlyingDatatype (0x40) DCP open flag used by FeedContentXattrOnly, which
// is suspected of causing CBS to silently drop mutations for body-only documents (CBG-4640).
//
// The test writes three document types that mirror production Sync Gateway usage:
//   - Body-only JSON doc (like _sync:user:* written via WriteCas/Insert)
//   - Xattr+body doc (like application documents)
//   - Counter doc (like _sync:seq written via Incr)
//
// Both backfill (docs written before feed starts) and live streaming (continuous,
// docs written after feed starts) scenarios are tested.
func TestDCPFeedContentBodyOnlyDocs(t *testing.T) {
	ctx := TestCtx(t)

	feedContentModes := []struct {
		name        string
		feedContent sgbucket.FeedContent
	}{
		{"FeedContentDefault", sgbucket.FeedContentDefault},
		{"FeedContentXattrOnly", sgbucket.FeedContentXattrOnly},
	}

	for _, mode := range feedContentModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, live := range []bool{false, true} {
				name := "backfill"
				if live {
					name = "live"
				}
				t.Run(name, func(t *testing.T) {
					bucket := GetTestBucket(t)
					defer bucket.Close(ctx)

					dataStore := bucket.GetSingleDataStore()

					bodyOnlyKey := t.Name() + "_bodyOnly"
					xattrKey := t.Name() + "_xattr"
					counterKey := t.Name() + "_counter"

					// writeTestDocs writes the three document types we want to verify
					writeTestDocs := func() {
						// Body-only JSON doc via WriteCas (Insert, cas=0) - same path as auth.Save() for _sync:user:*
						body := map[string]any{"type": "user", "name": "testuser", "channels": []string{"a", "b"}}
						_, err := dataStore.WriteCas(bodyOnlyKey, 0, 0, body, 0)
						require.NoError(t, err)

						// Xattr+body doc - same as application documents
						_, err = dataStore.WriteWithXattrs(ctx, xattrKey, 0, 0, []byte(`{"foo":"bar"}`), map[string][]byte{"_sync": []byte(`{"rev":"1-abc"}`)}, nil, nil)
						require.NoError(t, err)

						// Counter doc via Incr - same path as _sync:seq
						_, err = dataStore.Incr(counterKey, 1, 0, 0)
						require.NoError(t, err)
					}

					if !live {
						// Backfill: write docs before starting the feed
						writeTestDocs()
					}

					// Track which docs arrive
					var gotBodyOnly, gotXattr, gotCounter atomic.Bool
					var bodyOnlyEvent, xattrEvent, counterEvent sgbucket.FeedEvent
					var eventMu sync.Mutex
					allFound := make(chan struct{}, 1)

					callback := func(event sgbucket.FeedEvent) bool {
						eventMu.Lock()
						defer eventMu.Unlock()
						switch string(event.Key) {
						case bodyOnlyKey:
							bodyOnlyEvent = event
							bodyOnlyEvent.Key = append([]byte(nil), event.Key...)
							bodyOnlyEvent.Value = append([]byte(nil), event.Value...)
							gotBodyOnly.Store(true)
						case xattrKey:
							xattrEvent = event
							xattrEvent.Key = append([]byte(nil), event.Key...)
							xattrEvent.Value = append([]byte(nil), event.Value...)
							gotXattr.Store(true)
						case counterKey:
							counterEvent = event
							counterEvent.Key = append([]byte(nil), event.Key...)
							counterEvent.Value = append([]byte(nil), event.Value...)
							gotCounter.Store(true)
						default:
							return true
						}
						if gotBodyOnly.Load() && gotXattr.Load() && gotCounter.Load() {
							select {
							case allFound <- struct{}{}:
							default:
							}
						}
						return true
					}

					// Build FeedArguments for StartDCPFeed (works on both Rosmar and CBS)
					feedArgs := DCPClientOptions{
						FeedID:             t.Name(),
						Callback:           callback,
						CheckpointPrefix:   DefaultMetadataKeys.DCPCheckpointPrefix(t.Name()),
						CollectionNames:    NewCollectionNameSet(dataStore),
						FeedContent:        mode.feedContent,
						FromLatestSequence: live,
					}
					dcpClient, err := NewDCPClient(ctx, bucket, feedArgs)
					require.NoError(t, err)
					doneChan, err := dcpClient.Start()
					require.NoError(t, err)
					defer func() {
						assert.NoError(t, dcpClient.Close())
						RequireChanClosed(t, doneChan)
					}()

					if live {
						// Live streaming: write docs after the feed has started
						writeTestDocs()
					}

					// Wait for all three docs to arrive
					RequireChanRecv(t, allFound)

					// Verify the body-only doc arrived (this is the key assertion for CBG-4640)
					require.True(t, gotBodyOnly.Load(), "body-only document (like _sync:user:*) was not delivered by DCP with FeedContent=%s", mode.name)
					require.True(t, gotXattr.Load(), "xattr document was not delivered by DCP")
					require.True(t, gotCounter.Load(), "counter document was not delivered by DCP")

					// Verify event properties
					assert.Equal(t, sgbucket.FeedOpMutation, bodyOnlyEvent.Opcode)
					assert.Equal(t, sgbucket.FeedOpMutation, xattrEvent.Opcode)
					assert.Equal(t, sgbucket.FeedOpMutation, counterEvent.Opcode)

					assert.NotEqual(t, uint64(0), bodyOnlyEvent.Cas, "body-only doc CAS should be non-zero")
					assert.NotEqual(t, uint64(0), xattrEvent.Cas, "xattr doc CAS should be non-zero")
					assert.NotEqual(t, uint64(0), counterEvent.Cas, "counter doc CAS should be non-zero")

					switch mode.feedContent {
					case sgbucket.FeedContentDefault:
						// All docs should have bodies
						assert.NotEmpty(t, bodyOnlyEvent.Value, "body-only doc should have value with FeedContentDefault")
						assert.NotEmpty(t, xattrEvent.Value, "xattr doc should have value with FeedContentDefault")
					case sgbucket.FeedContentXattrOnly:
						// Body-only doc should have datatype preserved but body stripped
						assert.True(t, bodyOnlyEvent.DataType&sgbucket.FeedDataTypeJSON != 0 || bodyOnlyEvent.DataType == sgbucket.FeedDataTypeRaw,
							"body-only doc datatype should indicate JSON or raw, got %d", bodyOnlyEvent.DataType)
						// Xattr doc should have xattr data
						assert.True(t, xattrEvent.DataType&sgbucket.FeedDataTypeXattr != 0,
							"xattr doc should have xattr datatype flag, got %d", xattrEvent.DataType)
					}

					t.Logf("Results for FeedContent=%s %s:", mode.name, name)
					t.Logf("  bodyOnly: arrived=%v datatype=%d valueLen=%d cas=%d", gotBodyOnly.Load(), bodyOnlyEvent.DataType, len(bodyOnlyEvent.Value), bodyOnlyEvent.Cas)
					t.Logf("  xattr:    arrived=%v datatype=%d valueLen=%d cas=%d", gotXattr.Load(), xattrEvent.DataType, len(xattrEvent.Value), xattrEvent.Cas)
					t.Logf("  counter:  arrived=%v datatype=%d valueLen=%d cas=%d", gotCounter.Load(), counterEvent.DataType, len(counterEvent.Value), counterEvent.Cas)
				})
			}
		})
	}
}

func TestDCPClientAgentConfig(t *testing.T) {
	TestRequiresGocbDCPClient(t)
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	gocbv2Bucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)

	testCases := []struct {
		name         string
		serverSuffix string
		networkType  string
	}{
		{
			name:         "implicit",
			serverSuffix: "",
			networkType:  "",
		},
		{
			name:         "network=default",
			serverSuffix: "?network=default",
			networkType:  "default",
		},
		{
			name:         "network=external",
			serverSuffix: "?network=external",
			networkType:  "external",
		},
		{
			name:         "network=auto",
			serverSuffix: "?network=auto",
			networkType:  "auto",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotContains(t, gocbv2Bucket.Spec.Server, "?", "expected no query parameters for connection string to start")
			oldBucketSpecServer := gocbv2Bucket.Spec.Server
			defer func() { gocbv2Bucket.Spec.Server = oldBucketSpecServer }()
			gocbv2Bucket.Spec.Server += tc.serverSuffix
			dcpClient, err := NewGocbDCPClient(ctx,
				func(sgbucket.FeedEvent) bool { return true },
				GoCBDCPClientOptions{MetadataStoreType: DCPMetadataStoreInMemory},
				gocbv2Bucket)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, dcpClient.Close())
			}()

			config, err := dcpClient.getAgentConfig(gocbv2Bucket.GetSpec())
			require.NoError(t, err)

			require.Equal(t, tc.networkType, config.IoConfig.NetworkType)
		})
	}
}
