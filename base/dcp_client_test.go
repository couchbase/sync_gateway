package base

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"

	"github.com/stretchr/testify/assert"
)

func TestOneShotDCP(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	bucket := GetTestBucket(t)
	defer bucket.Close()

	num_docs := 1000
	// write documents to bucket
	body := map[string]interface{}{"foo": "bar"}
	for i := 0; i < num_docs; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		err := bucket.Set(key, 0, body)
		assert.NoError(t, err)
	}

	// create callback
	mutationCount := 0
	counterCallback := func(event sgbucket.FeedEvent) bool {
		mutationCount++
		return false
	}

	// start one shot feed
	store, ok := AsCouchbaseStore(bucket)
	assert.True(t, ok)
	feedID := t.Name()
	clientOptions := DCPClientOptions{
		OneShot: true,
	}

	dcpClient, err := NewDCPClient(feedID, counterCallback, clientOptions, store)
	assert.NoError(t, err)

	// Add additional documents in a separate goroutine, to verify afterEndSeq handling
	var additionalDocsWg sync.WaitGroup
	additionalDocsWg.Add(1)
	go func() {
		updatedBody := map[string]interface{}{"foo": "bar"}
		for i := num_docs; i < num_docs*2; i++ {
			key := fmt.Sprintf("%s_%d", t.Name(), i)
			err := bucket.Set(key, 0, updatedBody)
			assert.NoError(t, err)
		}
		additionalDocsWg.Done()
	}()

	doneChan, startErr := dcpClient.Start()
	assert.NoError(t, startErr)

	defer func() {
		_ = dcpClient.Close()
	}()

	// wait for done
	timeout := time.After(3 * time.Second)
	select {
	case err := <-doneChan:
		assert.Equal(t, num_docs, mutationCount)
		assert.NoError(t, err)
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
	mutationCount := 0
	counterCallback := func(event sgbucket.FeedEvent) bool {
		log.Printf("got mutation for: %s (vb: %d)", event.Key, event.VbNo)
		mutationCount++
		return false
	}

	// start continuous feed with terminator
	store, ok := AsCouchbaseStore(bucket)
	assert.True(t, ok)
	feedID := t.Name()

	dcpClient, err := NewDCPClient(feedID, counterCallback, DCPClientOptions{}, store)
	assert.NoError(t, err)

	// Add documents in a separate goroutine
	feedClosed := false
	var additionalDocsWg sync.WaitGroup
	additionalDocsWg.Add(1)
	go func() {
		updatedBody := map[string]interface{}{"foo": "bar"}
		for i := 0; i < 10000; i++ {
			if feedClosed {
				break
			}
			key := fmt.Sprintf("%s_%d", t.Name(), i)
			err := bucket.Set(key, 0, updatedBody)
			assert.NoError(t, err)
		}
		additionalDocsWg.Done()
	}()

	doneChan, startErr := dcpClient.Start()
	assert.NoError(t, startErr)

	// Wait for some processing to complete, then close the feed
	time.Sleep(10 * time.Millisecond)
	err = dcpClient.Close()
	assert.NoError(t, err)

	// wait for done
	timeout := time.After(3 * time.Second)
	select {
	case <-doneChan:
		feedClosed = true
	case <-timeout:
		t.Errorf("timeout waiting for one-shot feed to complete")
	}

	additionalDocsWg.Wait()
}
