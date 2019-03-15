package base

import (
	"testing"

	"github.com/couchbaselabs/go.assert"
)

// func TransformBucketCredentials(inputUsername, inputPassword, inputBucketname string) (username, password, bucketname string) {

func TestTransformBucketCredentials(t *testing.T) {

	inputUsername := "foo"
	inputPassword := "bar"
	inputBucketName := "baz"

	username, password, bucketname := TransformBucketCredentials(
		inputUsername,
		inputPassword,
		inputBucketName,
	)

	assert.Equals(t, username, inputUsername)
	assert.Equals(t, password, inputPassword)
	assert.Equals(t, bucketname, inputBucketName)

	inputUsername2 := ""
	inputPassword2 := "bar"
	inputBucketName2 := "baz"

	username2, password2, bucketname2 := TransformBucketCredentials(
		inputUsername2,
		inputPassword2,
		inputBucketName2,
	)

	assert.Equals(t, username2, inputBucketName2)
	assert.Equals(t, password2, inputPassword2)
	assert.Equals(t, bucketname2, inputBucketName2)

}

func TestDCPKeyFilter(t *testing.T) {

	assert.True(t, dcpKeyFilter([]byte("doc123")))
	assert.True(t, dcpKeyFilter([]byte("_sync:user:user1")))
	assert.True(t, dcpKeyFilter([]byte("_sync:role:role2")))
	assert.True(t, dcpKeyFilter([]byte("_sync:unusedSeq:1234")))

	assert.False(t, dcpKeyFilter([]byte("_sync:seq")))
	assert.False(t, dcpKeyFilter([]byte("_sync:unusualSeq")))
	assert.False(t, dcpKeyFilter([]byte("_sync:syncdata")))
	assert.False(t, dcpKeyFilter([]byte("_sync:dcp_ck:12")))
}
