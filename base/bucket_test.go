package base

import (
	"log"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestBucketSpec(t *testing.T) {

	bucketSpec := BucketSpec{
		Server:     "http://localhost:8091",
		Certpath:   "/myCertPath",
		Keypath:    "/my/key/path",
		CACertPath: "./myCACertPath",
	}

	connStr, err := bucketSpec.GetGoCBConnString()
	assertNoError(t, err, "Error creating connection string for bucket spec")
	assert.Equals(t, connStr, "http://localhost:8091?cacertpath=.%2FmyCACertPath&certpath=%2FmyCertPath&keypath=%2Fmy%2Fkey%2Fpath")

	// CACertPath not required
	bucketSpec.CACertPath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assertNoError(t, err, "Error creating connection string for bucket spec")
	assert.Equals(t, connStr, "http://localhost:8091?certpath=%2FmyCertPath&keypath=%2Fmy%2Fkey%2Fpath")

	// Certpath and keypath must both be defined - if either are missing, they shouldn't be included in connection string
	bucketSpec.CACertPath = "./myCACertPath"
	bucketSpec.Certpath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assertNoError(t, err, "Error creating connection string for bucket spec")
	assert.Equals(t, connStr, "http://localhost:8091?cacertpath=.%2FmyCACertPath")

	// Standard no-cert
	bucketSpec.CACertPath = ""
	bucketSpec.Certpath = ""
	bucketSpec.Keypath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assertNoError(t, err, "Error creating connection string for bucket spec")
	assert.Equals(t, connStr, "http://localhost:8091")

	// Bad URL should return error
	bucketSpec.Server = ":noScheme"
	connStr, err = bucketSpec.GetGoCBConnString()
	log.Printf("connstr: %s, err: %v", connStr, err)
	assertTrue(t, err != nil, "Expected error for bad URL")

}
