package rest

import (
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

// TestX509RoundtripUsingIP is a happy-path roundtrip write test for SG connecting to CBS using valid X.509 certs for authentication.
// The test enforces SG connects using an IP address which is also present in the node cert.
func TestX509RoundtripUsingIP(t *testing.T) {

	if !x509TestsEnabled() {
		t.Skipf("x509 tests not enabled via %s flag", x509TestFlag)
	}

	if base.UnitTestUrlIsWalrus() {
		t.Skip("X509 not supported in Walrus")
	}

	testURL, err := url.Parse(base.UnitTestUrl())
	require.NoError(t, err)
	testIP := net.ParseIP(testURL.Hostname())
	if testIP == nil {
		t.Skipf("Test requires %s to be an IP address, but had: %v", base.TestEnvCouchbaseServerUrl, testURL.Hostname())
	}
	assertHostnameMatch(t, testURL)

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()

	ca := generateX509CA(t)
	nodePair := generateX509Node(t, ca, []net.IP{testIP}, nil)
	sgPair := generateX509SG(t, ca, base.TbpClusterUsername(), time.Now().Add(time.Hour*24))
	teardownFn := saveX509Files(t, ca, nodePair, sgPair)
	defer teardownFn()

	err = loadCertsIntoCouchbaseServer(*testURL, ca, nodePair)
	require.NoError(t, err)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	// force couchbases:// scheme
	tb.BucketSpec.Server = "couchbases://" + testIP.String()
	// use x509 for auth
	tb.BucketSpec.Auth = base.NoPasswordAuthHandler{Handler: tb.BucketSpec.Auth}
	tb.BucketSpec.CACertPath = ca.PEMFilepath
	tb.BucketSpec.Certpath = sgPair.PEMFilepath
	tb.BucketSpec.Keypath = sgPair.KeyFilePath

	rt := NewRestTester(t, &RestTesterConfig{TestBucket: tb})
	defer rt.Close()

	// write a doc to ensure bucket ops work
	tr := rt.SendAdminRequest(http.MethodPut, "/db/"+t.Name(), `{"sgwrite":true}`)
	assertStatus(t, tr, http.StatusCreated)

	// wait for doc to come back over DCP
	err = rt.WaitForDoc(t.Name())
	require.NoError(t, err, "error waiting for doc over DCP")
}

// TestX509RoundtripUsingDomain is a happy-path roundtrip write test for SG connecting to CBS using valid X.509 certs for authentication.
// The test enforces SG connects using a domain name which is also present in the node cert.
func TestX509RoundtripUsingDomain(t *testing.T) {

	if !x509TestsEnabled() {
		t.Skipf("x509 tests not enabled via %s flag", x509TestFlag)
	}

	if base.UnitTestUrlIsWalrus() {
		t.Skip("X509 not supported in Walrus")
	}

	testURL, err := url.Parse(base.UnitTestUrl())
	require.NoError(t, err)
	testIP := net.ParseIP(testURL.Hostname())
	if testIP != nil {
		t.Skipf("Test requires %s to be a domain name, but had an IP: %v", base.TestEnvCouchbaseServerUrl, testURL.Hostname())
	}
	assertHostnameMatch(t, testURL)

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyHTTP, base.KeyCache, base.KeyDCP)()

	ca := generateX509CA(t)
	nodePair := generateX509Node(t, ca, nil, []string{testURL.Hostname()})
	sgPair := generateX509SG(t, ca, base.TbpClusterUsername(), time.Now().Add(time.Hour*24))
	teardownFn := saveX509Files(t, ca, nodePair, sgPair)
	defer teardownFn()

	err = loadCertsIntoCouchbaseServer(*testURL, ca, nodePair)
	require.NoError(t, err)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	// force couchbases:// scheme
	tb.BucketSpec.Server = "couchbases://" + testURL.Host
	// use x509 for auth
	tb.BucketSpec.Auth = base.NoPasswordAuthHandler{Handler: tb.BucketSpec.Auth}
	tb.BucketSpec.CACertPath = ca.PEMFilepath
	tb.BucketSpec.Certpath = sgPair.PEMFilepath
	tb.BucketSpec.Keypath = sgPair.KeyFilePath

	rt := NewRestTester(t, &RestTesterConfig{TestBucket: tb})
	defer rt.Close()

	// write a doc to ensure bucket ops work
	tr := rt.SendAdminRequest(http.MethodPut, "/db/"+t.Name(), `{"sgwrite":true}`)
	assertStatus(t, tr, http.StatusCreated)

	// wait for doc to come back over DCP
	err = rt.WaitForDoc(t.Name())
	require.NoError(t, err, "error waiting for doc over DCP")
}
