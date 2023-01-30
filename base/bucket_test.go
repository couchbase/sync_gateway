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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketSpec(t *testing.T) {
	queryTimeout := uint32(30)
	bucketSpec := BucketSpec{
		Server:               "http://localhost:8091",
		Certpath:             "/myCertPath",
		Keypath:              "/my/key/path",
		CACertPath:           "./myCACertPath",
		ViewQueryTimeoutSecs: &queryTimeout,
	}

	connStr, err := bucketSpec.GetGoCBConnString()
	assert.NoError(t, err, "Error creating connection string for bucket spec")
	assert.Equal(t, "http://localhost:8091?cacertpath=.%2FmyCACertPath&certpath=%2FmyCertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&keypath=%2Fmy%2Fkey%2Fpath&kv_pool_size="+DefaultGocbKvPoolSize+"&n1ql_timeout=30000&operation_tracing=false", connStr)

	// CACertPath not required
	bucketSpec.CACertPath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assert.NoError(t, err, "Error creating connection string for bucket spec")
	assert.Equal(t, "http://localhost:8091?certpath=%2FmyCertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&keypath=%2Fmy%2Fkey%2Fpath&kv_pool_size="+DefaultGocbKvPoolSize+"&n1ql_timeout=30000&operation_tracing=false", connStr)

	// Certpath and keypath must both be defined - if either are missing, they shouldn't be included in connection string
	bucketSpec.CACertPath = "./myCACertPath"
	bucketSpec.Certpath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assert.NoError(t, err, "Error creating connection string for bucket spec")
	assert.Equal(t, "http://localhost:8091?cacertpath=.%2FmyCACertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&kv_pool_size="+DefaultGocbKvPoolSize+"&n1ql_timeout=30000&operation_tracing=false", connStr)

	// Specify kv_pool_size in server
	bucketSpec.Server = "http://localhost:8091?kv_pool_size=2"
	connStr, err = bucketSpec.GetGoCBConnString()
	assert.NoError(t, err, "Error creating connection string for bucket spec")
	assert.Equal(t, "http://localhost:8091?cacertpath=.%2FmyCACertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&kv_pool_size=2&n1ql_timeout=30000&operation_tracing=false", connStr)

	// Standard no-cert
	bucketSpec.CACertPath = ""
	bucketSpec.Certpath = ""
	bucketSpec.Keypath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assert.NoError(t, err, "Error creating connection string for bucket spec")
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
				"host1:11210": map[string]string{
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
				"host1:11210": map[string]string{
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
				},
				"host2:11210": map[string]string{
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
				"host1:11210": map[string]string{
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
				},
				"host2:11210": map[string]string{
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
				},
				"host3:11210": map[string]string{
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
				},
				"host4:11210": map[string]string{
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
				"host1:11210": map[string]string{
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
				"host2:11210": map[string]string{
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
				"host1:11210": map[string]string{
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
				"host2:11210": map[string]string{
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
	assert.Equal(t, GoCBCustomSGTranscoder, ChooseCouchbaseDriver(DataBucket))
	assert.Equal(t, GoCB, ChooseCouchbaseDriver(IndexBucket))
	unknownCouchbaseBucketType := CouchbaseBucketType(math.MaxInt8)
	assert.Equal(t, GoCB, ChooseCouchbaseDriver(unknownCouchbaseBucketType))
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
	assert.True(t, fakeBucketSpec.IsTLS())
	fakeBucketSpec.Server = "couchbases"
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

func TestGetPoolName(t *testing.T) {
	fakeBucketSpec := &BucketSpec{}
	assert.Equal(t, "default", fakeBucketSpec.GetPoolName())
	fakeBucketSpec.PoolName = "Liverpool"
	assert.Equal(t, "Liverpool", fakeBucketSpec.GetPoolName())
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

	// Check TLSConfig with no CA certificate; InsecureSkipVerify should be true
	spec = BucketSpec{Certpath: clientCertPath, Keypath: clientKeyPath}
	conf = spec.TLSConfig()
	assert.NotEmpty(t, conf)
	assert.True(t, conf.InsecureSkipVerify)
	require.Len(t, conf.Certificates, 1)
	assert.Nil(t, conf.RootCAs)

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
