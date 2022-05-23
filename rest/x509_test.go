/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestX509RoundtripUsingIP is a happy-path roundtrip write test for SG connecting to CBS using valid X.509 certs for authentication.
// The test enforces SG connects using an IP address which is also present in the node cert.
func TestX509RoundtripUsingIP(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	tb, teardownFn, _, _, _ := setupX509Tests(t, true)
	defer tb.Close()
	defer teardownFn()

	rt := NewRestTester(t, &RestTesterConfig{TestBucket: tb, useTLSServer: true})
	defer rt.Close()

	// write a doc to ensure bucket ops work
	tr := rt.SendAdminRequest(http.MethodPut, "/db/"+t.Name(), `{"sgwrite":true}`)
	assertStatus(t, tr, http.StatusCreated)

	// wait for doc to come back over DCP
	err := rt.WaitForDoc(t.Name())
	require.NoError(t, err, "error waiting for doc over DCP")
}

// TestX509RoundtripUsingDomain is a happy-path roundtrip write test for SG connecting to CBS using valid X.509 certs for authentication.
// The test enforces SG connects using a domain name which is also present in the node cert.
func TestX509RoundtripUsingDomain(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	tb, teardownFn, _, _, _ := setupX509Tests(t, false)
	defer tb.Close()
	defer teardownFn()

	rt := NewRestTester(t, &RestTesterConfig{TestBucket: tb, useTLSServer: true})
	defer rt.Close()

	// write a doc to ensure bucket ops work
	tr := rt.SendAdminRequest(http.MethodPut, "/db/"+t.Name(), `{"sgwrite":true}`)
	assertStatus(t, tr, http.StatusCreated)

	// wait for doc to come back over DCP
	err := rt.WaitForDoc(t.Name())
	require.NoError(t, err, "error waiting for doc over DCP")
}

func TestX509UnknownAuthorityWrap(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	tb, teardownFn, _, _, _ := setupX509Tests(t, true)
	defer tb.Close()
	defer teardownFn()

	tb.BucketSpec.CACertPath = ""

	sc := DefaultStartupConfig("")

	username, password, _ := tb.BucketSpec.Auth.GetCredentials()

	sc.Bootstrap.Server = tb.BucketSpec.Server
	sc.Bootstrap.Username = username
	sc.Bootstrap.Password = password

	_, err := initClusterAgent(sc.Bootstrap.Server, sc.Bootstrap.Username, sc.Bootstrap.Password,
		sc.Bootstrap.X509CertPath, sc.Bootstrap.X509KeyPath, sc.Bootstrap.CACertPath, sc.Bootstrap.ServerTLSSkipVerify)
	assert.Error(t, err)

	assert.Contains(t, err.Error(), "Provide a CA cert, or set tls_skip_verify to true in config")
}

func TestAttachmentCompactionRun(t *testing.T) {
	tb, teardownFn, _, _, _ := setupX509Tests(t, true)
	defer tb.Close()
	defer teardownFn()

	rt := NewRestTester(t, &RestTesterConfig{TestBucket: tb, useTLSServer: true})
	defer rt.Close()

	for i := 0; i < 20; i++ {
		docID := fmt.Sprintf("testDoc-%d", i)
		attID := fmt.Sprintf("testAtt-%d", i)
		attBody := map[string]interface{}{"value": strconv.Itoa(i)}
		attJSONBody, err := base.JSONMarshal(attBody)
		assert.NoError(t, err)
		CreateLegacyAttachmentDoc(t, &db.Database{DatabaseContext: rt.GetDatabase()}, docID, []byte("{}"), attID, attJSONBody)
	}

	resp := rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)

	status := rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateCompleted)
	assert.Equal(t, int64(20), status.MarkedAttachments)
}

func setupX509Tests(t *testing.T, useIPAddress bool) (testBucket *base.TestBucket, teardownFunc func(), caCertPath string, certPath string, keyPath string) {
	if !x509TestsEnabled() {
		t.Skipf("x509 tests not enabled via %s flag", x509TestFlag)
	}

	if base.UnitTestUrlIsWalrus() {
		t.Skip("X509 not supported in Walrus")
	}

	testURL, err := url.Parse(base.UnitTestUrl())
	require.NoError(t, err)
	testIP := net.ParseIP(testURL.Hostname())
	if testIP == nil && useIPAddress {
		t.Skipf("Test requires %s to be an IP address, but had: %v", base.TestEnvCouchbaseServerUrl, testURL.Hostname())
	}

	if testIP != nil && !useIPAddress {
		t.Skipf("Test requires %s to be a domain name, but had an IP: %v", base.TestEnvCouchbaseServerUrl, testURL.Hostname())
	}

	assertHostnameMatch(t, testURL)

	ca := generateX509CA(t)

	var testIPs []net.IP
	var testURls []string

	if useIPAddress {
		testIPs = []net.IP{testIP}
	}

	if !useIPAddress {
		testURls = []string{testURL.Hostname()}
	}

	nodePair := generateX509Node(t, ca, testIPs, testURls)
	sgPair := generateX509SG(t, ca, base.TestClusterUsername(), time.Now().Add(time.Hour*24))
	teardownFn := saveX509Files(t, ca, nodePair, sgPair)

	isLocalX509, localUserName := base.TestX509LocalServer()
	if isLocalX509 {
		err = loadCertsIntoLocalCouchbaseServer(*testURL, ca, nodePair, localUserName)
	} else {
		err = loadCertsIntoCouchbaseServer(*testURL, ca, nodePair)
	}
	require.NoError(t, err)

	tb := base.GetTestBucket(t)

	// force couchbases:// scheme
	if useIPAddress {
		tb.BucketSpec.Server = "couchbases://" + testIP.String()
	} else {
		tb.BucketSpec.Server = "couchbases://" + testURL.Hostname()
	}

	caCertPath = ca.PEMFilepath
	certPath = sgPair.PEMFilepath
	keyPath = sgPair.KeyFilePath

	// use x509 for auth
	tb.BucketSpec.Auth = base.NoPasswordAuthHandler{Handler: tb.BucketSpec.Auth}
	tb.BucketSpec.CACertPath = caCertPath
	tb.BucketSpec.Certpath = certPath
	tb.BucketSpec.Keypath = keyPath

	return tb, teardownFn, caCertPath, certPath, keyPath
}
