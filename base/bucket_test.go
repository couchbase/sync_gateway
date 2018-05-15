package base

import (
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
	assert.Equals(t, connStr, "http://localhost:8091?cacertpath=.%2FmyCACertPath&certpath=%2FmyCertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&keypath=%2Fmy%2Fkey%2Fpath")

	// CACertPath not required
	bucketSpec.CACertPath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assertNoError(t, err, "Error creating connection string for bucket spec")
	assert.Equals(t, connStr, "http://localhost:8091?certpath=%2FmyCertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&keypath=%2Fmy%2Fkey%2Fpath")

	// Certpath and keypath must both be defined - if either are missing, they shouldn't be included in connection string
	bucketSpec.CACertPath = "./myCACertPath"
	bucketSpec.Certpath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assertNoError(t, err, "Error creating connection string for bucket spec")
	assert.Equals(t, connStr, "http://localhost:8091?cacertpath=.%2FmyCACertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256")

	// Standard no-cert
	bucketSpec.CACertPath = ""
	bucketSpec.Certpath = ""
	bucketSpec.Keypath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assertNoError(t, err, "Error creating connection string for bucket spec")

}
