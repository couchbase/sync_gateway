package base

import (
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
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
	goassert.Equals(t, username, inputUsername)
	goassert.Equals(t, password, inputPassword)
	goassert.Equals(t, bucketname, inputBucketName)

	inputUsername2 := ""
	inputPassword2 := "bar"
	inputBucketName2 := "baz"

	username2, password2, bucketname2 := TransformBucketCredentials(
		inputUsername2,
		inputPassword2,
		inputBucketName2,
	)

	goassert.Equals(t, username2, inputBucketName2)
	goassert.Equals(t, password2, inputPassword2)
	goassert.Equals(t, bucketname2, inputBucketName2)

}
