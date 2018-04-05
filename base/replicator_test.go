package base

import (
	"net/http"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

func TestReplicator(t *testing.T) {
	r := NewReplicator()
	assert.Equals(t, len(r.ActiveTasks()), 0)

	params := sgreplicate.ReplicationParameters{
		SourceDb:  "db1",
		Lifecycle: sgreplicate.CONTINUOUS,
	}
	_, err := r.Replicate(params, false)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(r.ActiveTasks()), 1)

	params = sgreplicate.ReplicationParameters{
		ReplicationId: "rep1",
		Lifecycle:     sgreplicate.CONTINUOUS,
	}
	_, err = r.Replicate(params, false)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(r.ActiveTasks()), 2)

	// now stop it
	_, err = r.Replicate(params, true)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(r.ActiveTasks()), 1)

	// stop all
	err = r.StopReplications()
	assert.Equals(t, err, nil)
	assert.Equals(t, len(r.ActiveTasks()), 0)
}

func TestReplicatorDuplicateID(t *testing.T) {
	r := NewReplicator()

	params := sgreplicate.ReplicationParameters{
		ReplicationId: "rep1",
		Lifecycle:     sgreplicate.CONTINUOUS,
	}
	_, err := r.Replicate(params, false)
	assert.Equals(t, err, nil)

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
	assert.Equals(t, err, nil)

	// Should get an error even if ReplicationIDs are different,
	// as they both share the same parameters.
	_, err = r.Replicate(params, false)
	assertHTTPError(t, err, http.StatusConflict)
}

func assertHTTPError(t *testing.T, err error, status int) {
	if httpErr, ok := err.(*HTTPError); !ok {
		assert.Errorf(t, "assertHTTPError: Expected an HTTP %d; got error %T %v", status, err, err)
	} else {
		assert.Equals(t, httpErr.Status, status)
	}
}
