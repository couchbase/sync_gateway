package base

import (
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestGetCredentials(t *testing.T) {
	auth := dcpAuth{
		Username:   "foo",
		Password:   "bar",
		BucketName: "baz",
	}

	username, password, bucketname := auth.GetCredentials()
	assert.Equals(t, username, auth.Username)
	assert.Equals(t, password, auth.Password)
	assert.Equals(t, bucketname, auth.BucketName)

	auth = dcpAuth{
		Password:   "bar",
		BucketName: "baz",
	}

	username, password, bucketname = auth.GetCredentials()
	assert.Equals(t, username, auth.BucketName)
	assert.Equals(t, password, auth.Password)
	assert.Equals(t, bucketname, auth.BucketName)

}
