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

// TestX509RoundtripForceIP is a happy-path roundtrip write test for SG connecting to CBS using valid X.509 certs for authentication.
// The test enforces SG connects using an IP address which is also present in the node cert.
func TestX509RoundtripForceIP(t *testing.T) {

	if !x509TestsEnabled() {
		t.Skipf("x509 tests not enabled via %s flag", x509TestFlag)
	}

	if base.UnitTestUrlIsWalrus() {
		t.Skip("X509 not supported in Walrus")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeyCache, base.KeyDCP)()

	// if the given test URL is a hostname, resolve the IP address, and use that for the cert and SG configuration.
	couchbaseServerURL, err := url.Parse(base.UnitTestUrl())
	require.NoError(t, err)
	serverIPAddr, err := net.ResolveIPAddr("ip", couchbaseServerURL.Hostname())
	require.NoError(t, err)

	caPEM, ca, caPrivKey := generateCACert(t)
	nodePEM, nodeKey := GenerateCBSNodeCert(t, ca, caPrivKey, []net.IP{serverIPAddr.IP}, nil)
	sgPEM, sgKey := GenerateSGClientCert(t, ca, caPrivKey, base.TbpClusterUsername(), time.Now().Add(time.Hour*24))

	caPEMFilepath,
		chainPEMFilepath, pkeyKeyFilepath,
		sgPEMFilepath, sgKeyFilepath,
		teardownFn := saveX509Files(t, caPEM, nodePEM, nodeKey, sgPEM, sgKey)
	defer teardownFn()

	err = loadCertsIntoCouchbaseServer(*couchbaseServerURL, caPEM, chainPEMFilepath, pkeyKeyFilepath)
	require.NoError(t, err)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	// override the server URL we're connecting to to match the IP in the cert
	tb.BucketSpec.Server = "couchbases://" + serverIPAddr.IP.String()
	// use x509 for auth
	tb.BucketSpec.Auth = base.NoPasswordAuthHandler{Handler: tb.BucketSpec.Auth}
	tb.BucketSpec.CACertPath = caPEMFilepath
	tb.BucketSpec.Certpath = sgPEMFilepath
	tb.BucketSpec.Keypath = sgKeyFilepath

	rt := NewRestTester(t, &RestTesterConfig{TestBucket: tb})
	defer rt.Close()

	// write a doc to ensure bucket ops work
	tr := rt.SendAdminRequest(http.MethodPut, "/db/"+t.Name(), `{"sgwrite":true}`)
	assertStatus(t, tr, http.StatusCreated)

	// wait for doc to come back over DCP
	err = rt.WaitForDoc(t.Name())
	require.NoError(t, err, "error waiting for doc over DCP")
}

// TestX509RoundtripForceDomain is a happy-path roundtrip write test for SG connecting to CBS using valid X.509 certs for authentication.
// The test enforces SG connects using a domain name which is also present in the node cert.
func TestX509RoundtripForceDomain(t *testing.T) {

	if !x509TestsEnabled() {
		t.Skipf("x509 tests not enabled via %s flag", x509TestFlag)
	}

	if base.UnitTestUrlIsWalrus() {
		t.Skip("X509 not supported in Walrus")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeyCache, base.KeyDCP)()

	// if the given test URL is a hostname, resolve the IP address, and use that for the cert and SG configuration.
	couchbaseServerURL, err := url.Parse(base.UnitTestUrl())
	require.NoError(t, err)
	serverIPAddr, err := net.ResolveIPAddr("ivp4", couchbaseServerURL.Hostname())
	require.NoError(t, err)

	serverDomain := domainForIPAddr(serverIPAddr.IP)

	caPEM, ca, caPrivKey := generateCACert(t)
	nodePEM, nodeKey := GenerateCBSNodeCert(t, ca, caPrivKey, nil, []string{serverDomain})
	sgPEM, sgKey := GenerateSGClientCert(t, ca, caPrivKey, base.TbpClusterUsername(), time.Now().Add(time.Hour*24))

	caPEMFilepath,
		chainPEMFilepath, pkeyKeyFilepath,
		sgPEMFilepath, sgKeyFilepath,
		teardownFn := saveX509Files(t, caPEM, nodePEM, nodeKey, sgPEM, sgKey)
	defer teardownFn()

	err = loadCertsIntoCouchbaseServer(*couchbaseServerURL, caPEM, chainPEMFilepath, pkeyKeyFilepath)
	require.NoError(t, err)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	// override the server URL we're connecting to to match the domain in the cert
	tb.BucketSpec.Server = "couchbases://" + serverDomain
	// use x509 for auth
	tb.BucketSpec.Auth = base.NoPasswordAuthHandler{Handler: tb.BucketSpec.Auth}
	tb.BucketSpec.CACertPath = caPEMFilepath
	tb.BucketSpec.Certpath = sgPEMFilepath
	tb.BucketSpec.Keypath = sgKeyFilepath

	rt := NewRestTester(t, &RestTesterConfig{TestBucket: tb})
	defer rt.Close()

	// write a doc to ensure bucket ops work
	tr := rt.SendAdminRequest(http.MethodPut, "/db/"+t.Name(), `{"sgwrite":true}`)
	assertStatus(t, tr, http.StatusCreated)

	// wait for doc to come back over DCP
	err = rt.WaitForDoc(t.Name())
	require.NoError(t, err, "error waiting for doc over DCP")
}
