package base

import (
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/go.assert"
)

func TestDedupeTapEventsLaterSeqSameDoc(t *testing.T) {

	tapEvents := []sgbucket.FeedEvent{
		{
			Opcode: sgbucket.FeedOpMutation,
			Key:    []byte("doc1"),
			Value:  []byte(`".."`),
			Cas:    1,
		},
		{
			Opcode: sgbucket.FeedOpMutation,
			Key:    []byte("doc1"),
			Value:  []byte(`".."`),
			Cas:    2,
		},
	}

	deduped := dedupeTapEvents(tapEvents)

	// make sure that one was deduped
	assert.Equals(t, len(deduped), 1)

	// make sure the earlier event was deduped
	dedupedEvent := deduped[0]
	assert.True(t, dedupedEvent.Cas == 2)

}

func TestDedupeNoDedupeDifferentDocs(t *testing.T) {

	tapEvents := []sgbucket.FeedEvent{
		{
			Opcode: sgbucket.FeedOpMutation,
			Key:    []byte("doc1"),
			Value:  []byte(`".."`),
			Cas:    1,
		},
		{
			Opcode: sgbucket.FeedOpMutation,
			Key:    []byte("doc2"),
			Value:  []byte(`".."`),
			Cas:    2,
		},
	}

	deduped := dedupeTapEvents(tapEvents)

	// make sure that nothing was deduped
	assert.True(t, len(deduped) == 2)

}
