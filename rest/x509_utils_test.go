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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

const (
	x509TestFlag            = "SG_TEST_X509"
	x509TestFlagSSHUsername = "SG_TEST_X509_SSH_USERNAME"
	x509SSHDefaultUsername  = "root"

	x509CACommonName = "SG Test Root CA"
)

// x509TestsEnabled returns true if the test flag is enabled.
func x509TestsEnabled() bool {
	b, _ := strconv.ParseBool(os.Getenv(x509TestFlag))
	if b == true {
		allVars := []string{base.TestEnvX509LocalUser, x509TestFlagSSHUsername, base.TestEnvCouchbaseServerDockerName}
		var setVars []string
		for _, envVar := range allVars {
			if os.Getenv(envVar) != "" {
				setVars = append(setVars, envVar)
			}
		}
		if len(setVars) > 1 {
			panic(fmt.Sprintf("%+v are mutually exclusive environment variables in a test environment", setVars))
		}
	}
	return b
}

// saveX509Files creates temp files for the given certs/keys and returns the full filepaths for each.
func saveX509Files(t *testing.T, ca *caPair, node *nodePair, sg *sgPair) {
	dirName := t.TempDir()

	caPEMFilepath := filepath.Join(dirName, "ca.pem")
	err := os.WriteFile(caPEMFilepath, ca.PEM.Bytes(), os.FileMode(0777))
	require.NoError(t, err)
	ca.PEMFilepath = caPEMFilepath

	chainPEMFilepath := filepath.Join(dirName, "chain.pem")
	err = os.WriteFile(chainPEMFilepath, node.PEM.Bytes(), os.FileMode(0777))
	require.NoError(t, err)
	node.PEMFilepath = chainPEMFilepath
	pkeyKeyFilepath := filepath.Join(dirName, "pkey.key")
	err = os.WriteFile(pkeyKeyFilepath, node.Key.Bytes(), os.FileMode(0777))
	require.NoError(t, err)
	node.KeyFilePath = pkeyKeyFilepath

	sgPEMFilepath := filepath.Join(dirName, "sg.pem")
	err = os.WriteFile(sgPEMFilepath, sg.PEM.Bytes(), os.FileMode(0777))
	require.NoError(t, err)
	sg.PEMFilepath = sgPEMFilepath
	sgKeyFilepath := filepath.Join(dirName, "sg.key")
	err = os.WriteFile(sgKeyFilepath, sg.Key.Bytes(), os.FileMode(0777))
	require.NoError(t, err)
	sg.KeyFilePath = sgKeyFilepath
}

// A set of types for sets of specific certs/keys so we can provide strongly-typed hints for their use (to prevent mixups)
type caPair struct {
	PEM         caCertType
	PEMFilepath string
	Cert        *x509.Certificate
	Key         *rsa.PrivateKey
}
type nodePair struct {
	PEM         nodeCertType
	PEMFilepath string
	Key         nodeKeyType
	KeyFilePath string
}
type sgPair struct {
	PEM         sgCertType
	PEMFilepath string
	Key         sgKeyType
	KeyFilePath string
}
type caCertType struct{ *bytes.Buffer }
type nodeCertType struct{ *bytes.Buffer }
type nodeKeyType struct{ *bytes.Buffer }
type sgCertType struct{ *bytes.Buffer }
type sgKeyType struct{ *bytes.Buffer }

func generateX509CA(t *testing.T) *caPair {
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caCert := &x509.Certificate{
		IsCA:                  true,
		BasicConstraintsValid: true,

		Subject: pkix.Name{CommonName: x509CACommonName},

		// Make the CA valid between yesterday and 10 years
		// This cert will be kept around on the server until the test next run, and we don't want it expiring
		NotBefore: time.Now().Add(time.Hour * -24),
		NotAfter:  time.Now().Add(time.Hour * 24 * 365 * 10),

		SerialNumber: big.NewInt(time.Now().UnixNano()),
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, caCert, caCert, &caPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	caPEM := new(bytes.Buffer)
	err = pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})
	require.NoError(t, err)

	return &caPair{
		PEM:  caCertType{caPEM},
		Cert: caCert,
		Key:  caPrivKey,
	}
}

type chainCertOpts struct {
	// isClientCert will configure the cert for use as a client cert if true, and server cert if false.
	isClientCert bool
	// expiryDate will be used for the 'NotAfter' field of the certificate. If unset, this will default to tomorrow for client certs, and 10 years for node certs.
	expiryDate time.Time
	// commonName valid only for SG client certs, it specifies the RBAC username to authorise
	commonName string
	// Below options are only valid for node certificates
	// sanIPs defines which IP addresses are valid to be serving this certificate via the SAN field
	sanIPs []net.IP
	// sanDNSNames defines which domain name are valid to be serving this certificate via the SAN field
	sanDNSNames []string
}

// generateX509SG generates an client cert for Sync Gateway to authorize with via X.509.
func generateX509SG(t *testing.T, ca *caPair, couchbaseUsername string, expiry time.Time) *sgPair {
	c, k := generateChainCert(t, ca, chainCertOpts{isClientCert: true, commonName: couchbaseUsername, expiryDate: expiry})
	return &sgPair{
		PEM: sgCertType{c},
		Key: sgKeyType{k},
	}
}

// generateX509Node is a convenience wrapper around generateChainCert for generating SG client certs.
func generateX509Node(t *testing.T, ca *caPair, sanIPs []net.IP, sanDNSNames []string) *nodePair {
	c, k := generateChainCert(t, ca, chainCertOpts{isClientCert: false, commonName: "cbs-node", sanIPs: sanIPs, sanDNSNames: sanDNSNames, expiryDate: time.Now().Add(time.Hour * 24 * 365 * 10)})
	return &nodePair{
		PEM: nodeCertType{c},
		Key: nodeKeyType{k},
	}
}

// generateChainCert will produce a client or server (CBS Node) certificate and key authorised by the given CA with the given options.
// Use generateX509Node or generateX509SG instead for ease of use.
func generateChainCert(t *testing.T, ca *caPair, opts chainCertOpts) (chainCert, chainKey *bytes.Buffer) {
	clientPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	chainCertTempl := &x509.Certificate{
		DNSNames:    opts.sanDNSNames,
		IPAddresses: opts.sanIPs,
		Subject:     pkix.Name{CommonName: opts.commonName},

		// Set NotBefore to yesterday to allow for clock skew
		NotBefore: time.Now().Add(time.Hour * -24),
		NotAfter:  opts.expiryDate,

		SerialNumber: big.NewInt(time.Now().UnixNano()),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}

	if opts.isClientCert {
		chainCertTempl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		chainCertTempl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}

	chainCertTempl.NotAfter = opts.expiryDate

	clientCertBytes, err := x509.CreateCertificate(rand.Reader, chainCertTempl, ca.Cert, &clientPrivateKey.PublicKey, ca.Key)
	require.NoError(t, err)

	chainCert = new(bytes.Buffer)
	err = pem.Encode(chainCert, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: clientCertBytes,
	})
	require.NoError(t, err)

	chainKey = new(bytes.Buffer)
	err = pem.Encode(chainKey, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(clientPrivateKey),
	})
	require.NoError(t, err)

	return chainCert, chainKey
}

// basicAuthRESTPIURLFromConnstrHost uses the given connstr to return a URL with embedded basic auth creds
// which can be used for requests against the Couchbase Server REST API.
func basicAuthRESTPIURLFromConnstrHost(connstrURL url.URL) url.URL {
	// override the scheme for http only
	connstrURL.Scheme = "http"
	// set basic auth creds
	connstrURL.User = url.UserPassword(base.TestClusterUsername(), base.TestClusterPassword())
	// append the http port if not set (e.g. was couchbase:// with no port)
	if connstrURL.Port() == "" {
		connstrURL.Host += ":8091"
	}
	return connstrURL
}

// x509SSHUsername returns the configured SSH username to provision X.509 certs to Couchbase Server
func x509SSHUsername() string {
	if u := os.Getenv(x509TestFlagSSHUsername); u != "" {
		return u
	}
	return x509SSHDefaultUsername
}

// loadCertsIntoCouchbaseServer will upload the given certs into Couchbase Server (via SSH and the REST API)
func loadCertsIntoCouchbaseServer(couchbaseServerURL url.URL, ca *caPair, node *nodePair) error {
	// Copy node cert and key via SSH
	sshRemoteHost := x509SSHUsername() + "@" + couchbaseServerURL.Hostname()
	err := sshCopyFileAsExecutable(node.PEMFilepath, sshRemoteHost, "/opt/couchbase/var/lib/couchbase/inbox")
	if err != nil {
		return err
	}
	logCtx := context.Background()
	base.DebugfCtx(logCtx, base.KeyAll, "copied x509 node chain.pem to integration test server")

	err = sshCopyFileAsExecutable(node.KeyFilePath, sshRemoteHost, "/opt/couchbase/var/lib/couchbase/inbox")
	if err != nil {
		return err
	}
	base.DebugfCtx(logCtx, base.KeyAll, "copied x509 node pkey.key to integration test server")

	return uploadCACertViaREST(couchbaseServerURL, ca)
}

// loadCertsIntoCouchbaseServer will upload the given certs into Couchbase Server (via SSH and the REST API)
func loadCertsIntoCouchbaseServerDocker(couchbaseServerURL url.URL, ca *caPair, node *nodePair, containerName string) error {
	err := copyLocalFileIntoDocker(containerName, node.PEMFilepath, "/opt/couchbase/var/lib/couchbase/inbox")
	if err != nil {
		return err
	}
	logCtx := context.Background()
	base.DebugfCtx(logCtx, base.KeyAll, "copied x509 node chain.pem to integration test server")

	err = copyLocalFileIntoDocker(containerName, node.KeyFilePath, "/opt/couchbase/var/lib/couchbase/inbox")
	if err != nil {
		return err
	}
	base.DebugfCtx(logCtx, base.KeyAll, "copied x509 node pkey.key to integration test server")

	return uploadCACertViaREST(couchbaseServerURL, ca)
}

// loadCertsIntoLocalCouchbaseServer will upload the given certs into Couchbase Server (via SSH and the REST API)
func loadCertsIntoLocalCouchbaseServer(couchbaseServerURL url.URL, ca *caPair, node *nodePair, localMacOSUser string) error {

	localMacOSCouchbaseServerInbox := "/Users/" + localMacOSUser + "/Library/Application Support/Couchbase/var/lib/couchbase/inbox"

	// Copy node cert and key
	err := copyLocalFile(node.PEMFilepath, localMacOSCouchbaseServerInbox)
	if err != nil {
		return err
	}
	logCtx := context.Background()
	base.DebugfCtx(logCtx, base.KeyAll, "copied x509 node chain.pem to integration test server")

	err = copyLocalFile(node.KeyFilePath, localMacOSCouchbaseServerInbox)
	if err != nil {
		return err
	}
	base.DebugfCtx(logCtx, base.KeyAll, "copied x509 node pkey.key to integration test server")
	return uploadCACertViaREST(couchbaseServerURL, ca)
}

func uploadCACertViaREST(couchbaseServerURL url.URL, ca *caPair) error {
	restAPIURL := basicAuthRESTPIURLFromConnstrHost(couchbaseServerURL)

	// Upload the CA cert via the REST API
	resp, err := http.Post(restAPIURL.String()+"/controller/uploadClusterCA", "application/octet-stream", ca.PEM)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("couldn't uploadClusterCA: expected %d status code but got %d: %s", http.StatusOK, resp.StatusCode, respBody)
	}
	logCtx := context.Background()
	base.DebugfCtx(logCtx, base.KeyAll, "uploaded ca.pem to Couchbase Server")

	// Make CBS read the newly uploaded certs
	resp, err = http.Post(restAPIURL.String()+"/node/controller/reloadCertificate", "", nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err = io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("couldn't reloadCertificate: expected %d status code but got %d: %s", http.StatusOK, resp.StatusCode, respBody)
	}
	base.DebugfCtx(logCtx, base.KeyAll, "triggered reload of certificates on Couchbase Server")

	if err := enableX509ClientCertsInCouchbaseServer(restAPIURL); err != nil {
		return err
	}

	return nil
}

// couchbaseNodeConfiguredHostname returns the Couchbase node name for the given URL.
func couchbaseNodeConfiguredHostname(restAPIURL url.URL) (string, error) {
	resp, err := http.Get(restAPIURL.String() + "/pools/default")
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("couldn't get node hostname: %d", resp.StatusCode)
	}

	type respStruct struct {
		NodesExt []struct {
			ThisNode bool   `json:"thisNode"`
			Hostname string `json:"hostname"`
		} `json:"nodes"`
	}
	var respJSON respStruct
	e := json.NewDecoder(resp.Body)
	if err := e.Decode(&respJSON); err != nil {
		return "", err
	}
	base.DebugfCtx(context.Background(), base.KeyAll, "enabled X.509 client certs in Couchbase Server")

	for _, n := range respJSON.NodesExt {
		if n.ThisNode {
			return n.Hostname, nil
		}
	}

	return "", fmt.Errorf("couldn't find 'thisNode' in nodeServices list: %v", respJSON)
}

// assertHostnameMatch ensures the hostname using for the test server matches what Couchbase Server's node hostname is.
func assertHostnameMatch(t *testing.T, couchbaseServerURL *url.URL) {
	restAPIURL := basicAuthRESTPIURLFromConnstrHost(*couchbaseServerURL)

	nodeHostname, err := couchbaseNodeConfiguredHostname(restAPIURL)
	require.NoError(t, err)
	if nodeHostname != restAPIURL.Host {
		t.Fatal("Test requires " + base.TestEnvCouchbaseServerUrl + " to be the same as the Couchbase Server node hostname...\n\n" +
			"Use `curl -X POST " + restAPIURL.String() + "/node/controller/rename -d hostname=" + restAPIURL.Hostname() + "` before running test")
	}
}

func enableX509ClientCertsInCouchbaseServer(restAPIURL url.URL) error {
	clientAuthSettings := bytes.NewBufferString(`
{
  "state": "enable",
  "prefixes": [{
    "path": "subject.cn",
    "prefix": "",
    "delimiter": ""
  }]
}`)

	// Configure CBS to enable optional X.509 client certs
	resp, err := http.Post(restAPIURL.String()+"/settings/clientCertAuth", "application/json", clientAuthSettings)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("couldn't configure clientCertAuth: expected %d or %d status codes but got %d: %s", http.StatusOK, http.StatusAccepted, resp.StatusCode, respBody)
	}
	base.DebugfCtx(context.Background(), base.KeyAll, "enabled X.509 client certs in Couchbase Server")

	return nil
}

// sshCopyFileAsExecutable takes in a full source filepath, an SSH remote host (e.g. root@localhost), and a destination directory.
// The destination directory will be created in full if it does not exist, the file will be copied, and then the read and execute permissions set.
func sshCopyFileAsExecutable(sourceFilepath, sshRemoteHost, destinationDirectory string) error {
	const (
		// SSH option flags
		forceKeyOnly        = "-o PasswordAuthentication=no"
		skipHostFingerprint = "-o StrictHostKeyChecking=no"
		batchMode           = "-o BatchMode=true"
	)

	// make destination directory if it doesn't exist
	err := runCmd("ssh", forceKeyOnly, skipHostFingerprint, batchMode, sshRemoteHost, "mkdir", "-p", destinationDirectory)
	if err != nil {
		return err
	}

	// copy the file (requires SSH Keys to be set up)
	err = runCmd("scp", forceKeyOnly, skipHostFingerprint, batchMode, sourceFilepath, sshRemoteHost+":"+destinationDirectory)
	if err != nil {
		return err
	}

	// make the file we just copied readable and executable (required for Couchbase server to use the certs)
	return runCmd("ssh", forceKeyOnly, skipHostFingerprint, batchMode, sshRemoteHost, "chmod -R a+rx", filepath.Join(destinationDirectory, filepath.Base(sourceFilepath)))
}

// copyLocalFile takes in a full source filepath and a destination directory.
// The destination directory will be created in full if it does not exist, the file will be copied, and then the read and execute permissions set.
func copyLocalFile(sourceFilepath, destinationDirectory string) error {

	// make destination directory if it doesn't exist
	err := runCmd("mkdir", "-p", destinationDirectory)
	if err != nil {
		return err
	}

	// copy the file
	err = runCmd("cp", sourceFilepath, destinationDirectory)
	if err != nil {
		return err
	}

	// make the file we just copied readable and executable (required for Couchbase server to use the certs)
	return runCmd("chmod", "-R", "a+rwx", filepath.Join(destinationDirectory, filepath.Base(sourceFilepath)))

}

// runCmd runs a command and returns error if the command fails
func runCmd(arg ...string) error {
	cmd := exec.Command(arg[0], arg[1:]...) //#nosec G204 // unsanitized input OK for test code
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Error executing %s : %s", cmd, output))
	}
	return nil
}

// copyLocalFileIntoDocker takes in a full source filepath and a destination directory.
// The destination directory will be created in full if it does not exist, the file will be copied, and then the read and execute permissions set.
func copyLocalFileIntoDocker(containerName, sourceFilepath, destinationDirectory string) error {

	// make destination directory if it doesn't exist
	err := runCmd("docker", "exec", containerName, "mkdir", "-p", destinationDirectory)
	if err != nil {
		return err
	}

	// copy the file
	err = runCmd("docker", "cp", sourceFilepath, containerName+":"+destinationDirectory)
	if err != nil {
		return err
	}

	// make the file we just copied readable and executable (required for Couchbase server to use the certs)
	return runCmd("docker", "exec", containerName, "chmod", "-R", "a+rwx", filepath.Join(destinationDirectory, filepath.Base(sourceFilepath)))
}
