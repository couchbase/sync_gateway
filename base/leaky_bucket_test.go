/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	goassert "github.com/couchbaselabs/go.assert"
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
	goassert.Equals(t, len(deduped), 1)

	// make sure the earlier event was deduped
	dedupedEvent := deduped[0]
	goassert.True(t, dedupedEvent.Cas == 2)

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
	goassert.True(t, len(deduped) == 2)

}
