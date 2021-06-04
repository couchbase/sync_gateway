/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"net/http"
	"testing"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicator(t *testing.T) {
	r := NewReplicator()
	require.Len(t, r.ActiveTasks(), 0)

	params := sgreplicate.ReplicationParameters{
		SourceDb:  "db1",
		Lifecycle: sgreplicate.CONTINUOUS,
	}
	_, err := r.Replicate(params, false)
	assert.Equal(t, nil, err)
	waitForActiveTasks(t, r, 1)

	params = sgreplicate.ReplicationParameters{
		ReplicationId: "rep1",
		Lifecycle:     sgreplicate.CONTINUOUS,
	}
	_, err = r.Replicate(params, false)
	assert.Equal(t, nil, err)
	waitForActiveTasks(t, r, 2)

	// now stop it
	_, err = r.Replicate(params, true)
	assert.Equal(t, nil, err)
	waitForActiveTasks(t, r, 1)

	// stop all
	err = r.StopReplications()
	assert.Equal(t, nil, err)
	waitForActiveTasks(t, r, 0)
}

func waitForActiveTasks(t *testing.T, r *Replicator, taskCount int) {
	for i := 0; i <= 20; i++ {
		if i == 20 {
			t.Fatalf("failed to find active task")
		}
		if len(r.ActiveTasks()) == taskCount {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func TestReplicatorDuplicateID(t *testing.T) {
	r := NewReplicator()

	params := sgreplicate.ReplicationParameters{
		ReplicationId: "rep1",
		Lifecycle:     sgreplicate.CONTINUOUS,
	}
	_, err := r.Replicate(params, false)
	assert.Equal(t, nil, err)

	// Should get an error because ReplicationIDs are identical.
	_, err = r.Replicate(params, false)
	assertHTTPError(t, err, http.StatusConflict)
}

func TestReplicatorDuplicateParams(t *testing.T) {
	r := NewReplicator()

	params := sgreplicate.ReplicationParameters{
		SourceDb:  "db1",
		Lifecycle: sgreplicate.CONTINUOUS,
	}
	_, err := r.Replicate(params, false)
	assert.Equal(t, nil, err)

	// Should get an error even if ReplicationIDs are different,
	// as they both share the same parameters.
	_, err = r.Replicate(params, false)
	assertHTTPError(t, err, http.StatusConflict)
}

func assertHTTPError(t *testing.T, err error, status int) {
	if httpErr, ok := err.(*HTTPError); !ok {
		assert.Fail(t, "assertHTTPError: Expected an HTTP %d; got error %T %v", status, err, err)
	} else {
		assert.Equal(t, status, httpErr.Status)
	}
}
