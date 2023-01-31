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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io/ioutil"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetGoCBConnString(t *testing.T) {

	queryTimeout := uint32(30)

	tests := []struct {
		name            string
		bucketSpec      BucketSpec
		expectedConnStr string
	}{
		{
			name: "v1 default values",
			bucketSpec: BucketSpec{
				CouchbaseDriver: GoCB,
				Server:          "http://localhost:8091",
			},
			expectedConnStr: "http://localhost:8091?http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&kv_pool_size=2&n1ql_timeout=75000&operation_tracing=false",
		},
		{
			name: "v1 no CA cert path",
			bucketSpec: BucketSpec{
				CouchbaseDriver:      GoCB,
				Server:               "http://localhost:8091?custom=true&kv_pool_size=3",
				ViewQueryTimeoutSecs: &queryTimeout,
				Certpath:             "/myCertPath",
				Keypath:              "/my/key/path",
			},
			expectedConnStr: "http://localhost:8091?certpath=%2FmyCertPath&custom=true&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&keypath=%2Fmy%2Fkey%2Fpath&kv_pool_size=3&n1ql_timeout=30000&operation_tracing=false",
		},
		{
			name: "v1 missing keypath should omit certpath",
			bucketSpec: BucketSpec{
				CouchbaseDriver:      GoCB,
				Server:               "http://localhost:8091?custom=true&kv_pool_size=3",
				ViewQueryTimeoutSecs: &queryTimeout,
				Certpath:             "/myCertPath",
			},
			expectedConnStr: "http://localhost:8091?custom=true&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&kv_pool_size=3&n1ql_timeout=30000&operation_tracing=false",
		},
		{
			name: "v1 all values",
			bucketSpec: BucketSpec{
				CouchbaseDriver:      GoCB,
				Server:               "http://localhost:8091?custom=true&kv_pool_size=3",
				ViewQueryTimeoutSecs: &queryTimeout,
				Certpath:             "/myCertPath",
				Keypath:              "/my/key/path",
				CACertPath:           "./myCACertPath",
			},
			expectedConnStr: "http://localhost:8091?cacertpath=.%2FmyCACertPath&certpath=%2FmyCertPath&custom=true&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&keypath=%2Fmy%2Fkey%2Fpath&kv_pool_size=3&n1ql_timeout=30000&operation_tracing=false",
		},
		{
			name: "v2 default values",
			bucketSpec: BucketSpec{
				CouchbaseDriver: GoCBv2,
				Server:          "http://localhost:8091",
			},
			expectedConnStr: "http://localhost:8091?idle_http_connection_timeout=90000&kv_pool_size=2&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
		},
		{
			name: "v2 no CA cert path",
			bucketSpec: BucketSpec{
				CouchbaseDriver:      GoCBv2,
				Server:               "http://localhost:8091?custom=true&kv_pool_size=3",
				ViewQueryTimeoutSecs: &queryTimeout,
			},
			expectedConnStr: "http://localhost:8091?custom=true&idle_http_connection_timeout=90000&kv_pool_size=3&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
		},
		{
			name: "v2 all values",
			bucketSpec: BucketSpec{
				CouchbaseDriver:      GoCBv2,
				Server:               "http://localhost:8091?custom=true&kv_pool_size=3",
				ViewQueryTimeoutSecs: &queryTimeout,
				Certpath:             "/myCertPath",
				Keypath:              "/my/key/path",
				CACertPath:           "./myCACertPath",
			},
			expectedConnStr: "http://localhost:8091?ca_cert_path=.%2FmyCACertPath&custom=true&idle_http_connection_timeout=90000&kv_pool_size=3&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualConnStr, err := test.bucketSpec.GetGoCBConnString()
			assert.NoError(t, err, "Unexpected error creating connection string for bucket spec")
			assert.Equal(t, test.expectedConnStr, actualConnStr)
		})
	}
}

func TestGetStatsVbSeqno(t *testing.T) {

	// We'll artificially lower this here to make for easier test data
	const maxVbno = 4

	tests := []struct {
		name               string
		stats              map[string]map[string]string
		expectedUUIDs      map[uint16]uint64
		expectedHighSeqnos map[uint16]uint64
	}{
		{
			name: "1 node",
			stats: map[string]map[string]string{
				"host1:11210": {
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "993",
				},
			},
			expectedUUIDs: map[uint16]uint64{
				0: 90,
				1: 91,
				2: 92,
				3: 93,
			},
			expectedHighSeqnos: map[uint16]uint64{
				0: 990,
				1: 991,
				2: 992,
				3: 993,
			},
		},
		{
			name: "2 nodes",
			stats: map[string]map[string]string{
				"host1:11210": {
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
				},
				"host2:11210": {
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "993",
				},
			},
			expectedUUIDs: map[uint16]uint64{
				0: 90,
				1: 91,
				2: 92,
				3: 93,
			},
			expectedHighSeqnos: map[uint16]uint64{
				0: 990,
				1: 991,
				2: 992,
				3: 993,
			},
		},
		{
			name: "4 nodes",
			stats: map[string]map[string]string{
				"host1:11210": {
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
				},
				"host2:11210": {
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
				},
				"host3:11210": {
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
				},
				"host4:11210": {
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "993",
				},
			},
			expectedUUIDs: map[uint16]uint64{
				0: 90,
				1: 91,
				2: 92,
				3: 93,
			},
			expectedHighSeqnos: map[uint16]uint64{
				0: 990,
				1: 991,
				2: 992,
				3: 993,
			},
		},
		{
			name: "2 nodes with replica vbuckets",
			stats: map[string]map[string]string{
				"host1:11210": {
					// active vbuckets
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
					// replica vbuckets
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "993",
				},
				"host2:11210": {
					// active vbuckets
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "993",
					// replica vbuckets
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
				},
			},
			expectedUUIDs: map[uint16]uint64{
				0: 90,
				1: 91,
				2: 92,
				3: 93,
			},
			expectedHighSeqnos: map[uint16]uint64{
				0: 990,
				1: 991,
				2: 992,
				3: 993,
			},
		},
		{
			name: "2 nodes with lagging replica vbuckets",
			stats: map[string]map[string]string{
				"host1:11210": {
					// active vbuckets
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
					// slightly behind replicas
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "982",
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "983",
				},
				"host2:11210": {
					// active vbuckets
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "993",
					// slightly behind replicas
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "980",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "981",
				},
			},
			expectedUUIDs: map[uint16]uint64{
				0: 90,
				1: 91,
				2: 92,
				3: 93,
			},
			expectedHighSeqnos: map[uint16]uint64{
				0: 990,
				1: 991,
				2: 992,
				3: 993,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(ts *testing.T) {
			actualUUIDs, actualHighSeqnos, err := GetStatsVbSeqno(test.stats, maxVbno, false)
			assert.NoError(ts, err)
			assert.Equal(t, test.expectedUUIDs, actualUUIDs)
			assert.Equal(t, test.expectedHighSeqnos, actualHighSeqnos)
		})
	}
}

func TestChooseCouchbaseDriver(t *testing.T) {
	assert.Equal(t, GoCBv2, ChooseCouchbaseDriver(DataBucket))
	assert.Equal(t, GoCBv2, ChooseCouchbaseDriver(IndexBucket))
	unknownCouchbaseBucketType := CouchbaseBucketType(math.MaxInt8)
	assert.Equal(t, GoCBv2, ChooseCouchbaseDriver(unknownCouchbaseBucketType))
}

func TestCouchbaseDriverToString(t *testing.T) {
	assert.Equal(t, "GoCB", GoCB.String())
	assert.Equal(t, "GoCBCustomSGTranscoder", GoCBCustomSGTranscoder.String())
	unknownCouchbaseDriver := CouchbaseDriver(math.MaxInt8)
	assert.Equal(t, "UnknownCouchbaseDriver", unknownCouchbaseDriver.String())
}

func TestIsTLS(t *testing.T) {
	fakeBucketSpec := &BucketSpec{}
	fakeBucketSpec.Server = "http://localhost:8091"
	assert.False(t, fakeBucketSpec.IsTLS())
	fakeBucketSpec.Server = "https://localhost:443"
	assert.False(t, fakeBucketSpec.IsTLS())
	fakeBucketSpec.Server = "couchbase://localhost"
	assert.False(t, fakeBucketSpec.IsTLS())
	fakeBucketSpec.Server = "couchbases://localhost"
	assert.True(t, fakeBucketSpec.IsTLS())
}

func TestUseClientCert(t *testing.T) {
	fakeBucketSpec := &BucketSpec{}
	assert.False(t, fakeBucketSpec.UseClientCert())
	fakeBucketSpec.Certpath = "/var/lib/couchbase/inbox/ca.pem"
	assert.False(t, fakeBucketSpec.UseClientCert())
	fakeBucketSpec.Keypath = "/var/lib/couchbase/inbox/pkey.key"
	assert.True(t, fakeBucketSpec.UseClientCert())
}

func TestGetViewQueryTimeout(t *testing.T) {
	fakeBucketSpec := &BucketSpec{}
	expectedViewQueryTimeout := 75 * time.Second
	assert.Equal(t, expectedViewQueryTimeout, fakeBucketSpec.GetViewQueryTimeout())
	viewQueryTimeoutSecs := uint32(0)
	fakeBucketSpec.ViewQueryTimeoutSecs = &viewQueryTimeoutSecs
	expectedViewQueryTimeout = 10 * 365 * 24 * time.Hour // 10 Years
	assert.Equal(t, expectedViewQueryTimeout, fakeBucketSpec.GetViewQueryTimeout())
}

func mockCertificatesAndKeys(t *testing.T) (certPath, clientCertPath, clientKeyPath, rootCertPath, rootKeyPath string) {
	certPath, err := ioutil.TempDir("", "certs")
	require.NoError(t, err, "Temp directory should be created")

	rootKeyPath = filepath.Join(certPath, "root.key")
	rootCertPath = filepath.Join(certPath, "root.pem")
	clientKeyPath = filepath.Join(certPath, "client.key")
	clientCertPath = filepath.Join(certPath, "client.pem")

	notBefore := time.Now().Add(time.Duration(-2) * time.Hour)
	notAfter := time.Now().Add(time.Duration(2) * time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	assert.NoError(t, err, "Serial number should be generated")

	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err, "Root key should be generated")
	saveAsKeyFile(t, rootKeyPath, rootKey)

	rootTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Couchbase, Inc."},
			CommonName:   "Root CA",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, &rootTemplate, &rootTemplate, &rootKey.PublicKey, rootKey)
	require.NoError(t, err, "Root CA certificate should be generated")
	saveAsCertFile(t, rootCertPath, certBytes)

	clientTemplate := x509.Certificate{
		SerialNumber: new(big.Int).SetInt64(4),
		Subject: pkix.Name{
			Organization: []string{"Couchbase, Inc."},
			CommonName:   "client_auth_test_cert",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	saveAsKeyFile(t, clientKeyPath, clientKey)
	certBytes, err = x509.CreateCertificate(rand.Reader, &clientTemplate, &rootTemplate, &clientKey.PublicKey, rootKey)
	require.NoError(t, err, "Client certificate should be generated")
	saveAsCertFile(t, clientCertPath, certBytes)

	require.True(t, FileExists(rootKeyPath), "File %v should exists", rootKeyPath)
	require.True(t, FileExists(rootCertPath), "File %v should exists", rootCertPath)
	require.True(t, FileExists(clientKeyPath), "File %v should exists", clientKeyPath)
	require.True(t, FileExists(clientCertPath), "File %v should exists", clientCertPath)

	return certPath, clientCertPath, clientKeyPath, rootCertPath, rootKeyPath
}

func saveAsKeyFile(t *testing.T, filename string, key *ecdsa.PrivateKey) {
	file, err := os.Create(filename)
	require.NoError(t, err)
	b, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	err = pem.Encode(file, &pem.Block{Type: "EC PRIVATE KEY", Bytes: b})
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func saveAsCertFile(t *testing.T, filename string, derBytes []byte) {
	certOut, err := os.Create(filename)
	require.NoError(t, err, "No error while opening cert.pem for writing")
	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	require.NoError(t, err, "No error while writing data to cert.pem")
	err = certOut.Close()
	require.NoError(t, err, "No error while closing cert.pem")
}

func TestTLSConfig(t *testing.T) {
	// Mock fake root CA and client certificates for verification
	certPath, clientCertPath, clientKeyPath, rootCertPath, rootKeyPath := mockCertificatesAndKeys(t)

	// Remove the keys and certificates after verification
	defer func() {
		assert.NoError(t, os.RemoveAll(certPath))
		assert.False(t, DirExists(certPath), "Directory: %v shouldn't exists", certPath)
	}()

	// Simulate error creating tlsConfig for DCP processing
	spec := BucketSpec{
		Server:     "http://localhost:8091",
		Certpath:   "/var/lib/couchbase/unknown.client.cert",
		Keypath:    "/var/lib/couchbase/unknown.client.key",
		CACertPath: "/var/lib/couchbase/unknown.root.ca.pem",
	}
	conf := spec.TLSConfig()
	assert.Nil(t, conf)

	// Simulate valid configuration scenario with fake mocked certificates and keys;
	spec = BucketSpec{Certpath: clientCertPath, Keypath: clientKeyPath, CACertPath: rootCertPath}
	conf = spec.TLSConfig()
	assert.NotEmpty(t, conf)
	assert.NotNil(t, conf.RootCAs)
	require.Len(t, conf.Certificates, 1)
	assert.False(t, conf.InsecureSkipVerify)

	// Check TLSConfig with no CA certificate, and TlsSkipVerify true; InsecureSkipVerify should be true
	spec = BucketSpec{TLSSkipVerify: true, Certpath: clientCertPath, Keypath: clientKeyPath}
	conf = spec.TLSConfig()
	assert.NotEmpty(t, conf)
	assert.True(t, conf.InsecureSkipVerify)
	require.Len(t, conf.Certificates, 1)
	assert.Nil(t, conf.RootCAs)

	// Check TLSConfig with no certificates provided, and TlsSkipVerify true. InsecureSkipVerify should be true and fields should be nil CBG-1518
	spec = BucketSpec{TLSSkipVerify: true}
	conf = spec.TLSConfig()
	assert.NotEmpty(t, conf)
	assert.True(t, conf.InsecureSkipVerify)
	assert.Nil(t, conf.RootCAs)
	assert.Nil(t, conf.Certificates)

	// Check TLSConfig with no certs provided. InsecureSkipVerify should always be false. Should be empty config on Windows CBG-1518
	spec = BucketSpec{}
	conf = spec.TLSConfig()
	assert.NotEmpty(t, conf)
	assert.False(t, conf.InsecureSkipVerify)
	require.NotNil(t, conf.RootCAs)
	if runtime.GOOS != "windows" {
		assert.NotEqual(t, x509.NewCertPool(), conf.RootCAs)
	} else {
		assert.Equal(t, x509.NewCertPool(), conf.RootCAs)
	}

	// Check TLSConfig by providing invalid root CA certificate; provide root certificate key path
	// instead of root CA certificate. It should throw "can't append certs from PEM" error.
	spec = BucketSpec{Certpath: clientCertPath, Keypath: clientKeyPath, CACertPath: rootKeyPath}
	conf = spec.TLSConfig()
	assert.Empty(t, conf)

	// Provide invalid client certificate key along with valid certificate; It should fail while
	// trying to add key and certificate to config as x509 key pair;
	spec = BucketSpec{Certpath: clientCertPath, Keypath: rootKeyPath, CACertPath: rootCertPath}
	conf = spec.TLSConfig()
	assert.Empty(t, conf)
}

func TestBaseBucket(t *testing.T) {

	tests := []struct {
		name   string
		bucket Bucket
	}{
		{
			name:   "gocb",
			bucket: &CouchbaseBucketGoCB{},
		},
		{
			name:   "leaky",
			bucket: &LeakyBucket{bucket: &CouchbaseBucketGoCB{}},
		},
		{
			name:   "logging",
			bucket: &LoggingBucket{bucket: &CouchbaseBucketGoCB{}},
		},
		{
			name:   "leakyLogging",
			bucket: &LeakyBucket{bucket: &LoggingBucket{bucket: &CouchbaseBucketGoCB{}}},
		},
		{
			name:   "loggingLeaky",
			bucket: &LoggingBucket{bucket: &LeakyBucket{bucket: &CouchbaseBucketGoCB{}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			baseBucket := GetBaseBucket(test.bucket)
			_, ok := baseBucket.(*CouchbaseBucketGoCB)
			assert.True(t, ok, "Base bucket wasn't gocb")
		})
	}
}

func TestBucketSpecIsWalrusBucket(t *testing.T) {
	tests := []struct {
		spec     BucketSpec
		expected bool
	}{
		{
			spec:     BucketSpec{Server: ""},
			expected: false,
		},
		{
			spec:     BucketSpec{Server: "walrus:"},
			expected: true,
		},
		{
			spec:     BucketSpec{Server: "file:"},
			expected: true,
		},
		{
			spec:     BucketSpec{Server: "/foo"},
			expected: true,
		},
		{
			spec:     BucketSpec{Server: "./foo"},
			expected: true,
		},
		{
			spec:     BucketSpec{Server: "couchbase://walrus"},
			expected: false,
		},
		{
			spec:     BucketSpec{Server: "http://walrus:8091"},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.spec.Server, func(t *testing.T) {
			assert.Equal(t, test.expected, test.spec.IsWalrusBucket())
		})
	}

}
