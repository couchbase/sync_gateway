package rest

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
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
	return b
}

// saveX509Files creates temp files for the given certs/keys and returns the full filepaths for each.
func saveX509Files(t *testing.T, caPEM, chainPEM, pkeyKey, sgPEM, sgKey *bytes.Buffer) (caPEMFilepath, chainPEMFilepath, pkeyKeyFilepath, sgPEMFilepath, sgKeyFilepath string, teardownFn func()) {
	dirName, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)

	caPEMFilepath = filepath.Join(dirName, "ca.pem")
	err = ioutil.WriteFile(caPEMFilepath, caPEM.Bytes(), os.FileMode(777))
	require.NoError(t, err)

	chainPEMFilepath = filepath.Join(dirName, "chain.pem")
	err = ioutil.WriteFile(chainPEMFilepath, chainPEM.Bytes(), os.FileMode(777))
	require.NoError(t, err)
	pkeyKeyFilepath = filepath.Join(dirName, "pkey.key")
	err = ioutil.WriteFile(pkeyKeyFilepath, pkeyKey.Bytes(), os.FileMode(777))
	require.NoError(t, err)

	sgPEMFilepath = filepath.Join(dirName, "sg.pem")
	err = ioutil.WriteFile(sgPEMFilepath, sgPEM.Bytes(), os.FileMode(777))
	require.NoError(t, err)
	sgKeyFilepath = filepath.Join(dirName, "sg.key")
	err = ioutil.WriteFile(sgKeyFilepath, sgKey.Bytes(), os.FileMode(777))
	require.NoError(t, err)

	return caPEMFilepath, chainPEMFilepath, pkeyKeyFilepath, sgPEMFilepath, sgKeyFilepath, func() {
		_ = os.RemoveAll(dirName)
	}
}

func generateCACert(t *testing.T) (caPEM *bytes.Buffer, ca *x509.Certificate, caPrivKey *rsa.PrivateKey) {
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	ca = &x509.Certificate{
		IsCA:                  true,
		BasicConstraintsValid: true,

		Subject: pkix.Name{CommonName: x509CACommonName},

		// Make the CA valid between yesterday and 10 years
		// This cert will be kept around on the server until the test next run, and we don't want it expiring
		NotBefore: time.Now().Add(time.Hour * -24),
		NotAfter:  time.Now().Add(time.Hour * 24 * 365 * 10),

		SerialNumber: big.NewInt(time.Now().UnixNano()),
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	caPEM = new(bytes.Buffer)
	err = pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})
	require.NoError(t, err)

	return caPEM, ca, caPrivKey
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

// GenerateSGClientCert generates an client cert for Sync Gateway to authorize with via X.509.
func GenerateSGClientCert(t *testing.T, ca *x509.Certificate, caPrivKey *rsa.PrivateKey, couchbaseUsername string, expiry time.Time) (clientPEM, clientKey *bytes.Buffer) {
	return generateChainCert(t, ca, caPrivKey, chainCertOpts{isClientCert: true, commonName: couchbaseUsername, expiryDate: expiry})
}

// GenerateCBSNodeCert is a convenience wrapper around generateChainCert for generating SG client certs.
func GenerateCBSNodeCert(t *testing.T, ca *x509.Certificate, caPrivKey *rsa.PrivateKey, sanIPs []net.IP, sanDNSNames []string) (clientPEM, clientKey *bytes.Buffer) {
	return generateChainCert(t, ca, caPrivKey, chainCertOpts{isClientCert: false, sanIPs: sanIPs, sanDNSNames: sanDNSNames, expiryDate: time.Now().Add(time.Hour * 24 * 365 * 10)})
}

// generateChainCert will produce a client or server (CBS Node) certificate and key authorised by the given CA with the given options.
// Use generateCBSNodeCert or generateSGClientCert instead for ease of use.
func generateChainCert(t *testing.T, ca *x509.Certificate, caPrivKey *rsa.PrivateKey, opts chainCertOpts) (clientPEM, clientKey *bytes.Buffer) {
	clientPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	clientCertTmpl := &x509.Certificate{
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
		clientCertTmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		clientCertTmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}

	clientCertTmpl.NotAfter = opts.expiryDate

	clientCertBytes, err := x509.CreateCertificate(rand.Reader, clientCertTmpl, ca, &clientPrivateKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	clientPEM = new(bytes.Buffer)
	err = pem.Encode(clientPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: clientCertBytes,
	})
	require.NoError(t, err)

	clientKey = new(bytes.Buffer)
	err = pem.Encode(clientKey, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(clientPrivateKey),
	})
	require.NoError(t, err)

	return clientPEM, clientKey
}

// connStrURLToRESTAPIURL parses the given connstr and returns a URL which can be used for the Couchbase Server REST API.
func connStrURLToRESTAPIURL(connstrURL url.URL) url.URL {
	// override the scheme for http only
	connstrURL.Scheme = "http"
	// set basic auth creds
	connstrURL.User = url.UserPassword(base.TbpClusterUsername(), base.TbpClusterPassword())
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
func loadCertsIntoCouchbaseServer(couchbaseServerURL url.URL, caPEM *bytes.Buffer, chainPEMFilepath, pkeyKeyFilepath string) error {
	// Copy node cert and key via SSH
	sshRemoteHost := x509SSHUsername() + "@" + couchbaseServerURL.Hostname()
	err := sshCopyFileAsExecutable(chainPEMFilepath, sshRemoteHost, "/opt/couchbase/var/lib/couchbase/inbox")
	if err != nil {
		return err
	}
	base.Debugf(base.KeyAll, "copied x509 node chain.pem to integration test server")

	err = sshCopyFileAsExecutable(pkeyKeyFilepath, sshRemoteHost, "/opt/couchbase/var/lib/couchbase/inbox")
	if err != nil {
		return err
	}
	base.Debugf(base.KeyAll, "copied x509 node pkey.key to integration test server")

	// Upload the CA cert via the REST API
	restAPIURL := connStrURLToRESTAPIURL(couchbaseServerURL)
	resp, err := http.Post(restAPIURL.String()+"/controller/uploadClusterCA", "application/octet-stream", caPEM)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("expected %q status code but got %q: %s", http.StatusOK, resp.StatusCode, respBody)
	}
	base.Debugf(base.KeyAll, "uploaded ca.pem to couchbase server")

	// Make CBS read the newly uploaded certs
	resp, err = http.Post(restAPIURL.String()+"/node/controller/reloadCertificate", "", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("expected %q status code but got %q: %s", http.StatusOK, resp.StatusCode, respBody)
	}
	base.Debugf(base.KeyAll, "triggered reload of certificates on couchbase server")

	return nil
}

// sshCopyFileAsExecutable takes in a full source filepath, an SSH remote host (e.g. root@localhost), and a destination directory.
// The destination directory will be created in full if it does not exist, the file will be copied, and then the read and execute permissions set.
func sshCopyFileAsExecutable(sourceFilepath, sshRemoteHost, destinationDirectory string) error {
	const (
		// SSH option flags
		forceKeyOnly        = "-o PasswordAuthentication=no"
		skipHostFingerprint = "-o StrictHostKeyChecking=no"
	)

	// make destination directory if it doesn't exist
	cmd := exec.Command("ssh", forceKeyOnly, skipHostFingerprint, sshRemoteHost, "mkdir", "-p", destinationDirectory)
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrap(err, string(output))
	}

	// copy the file (requires SSH Keys to be set up)
	cmd = exec.Command("scp", forceKeyOnly, skipHostFingerprint, sourceFilepath, sshRemoteHost+":"+destinationDirectory)
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrap(err, string(output))
	}

	// make the file we just copied readable and executable (required for Couchbase server to use the certs)
	cmd = exec.Command("ssh", forceKeyOnly, skipHostFingerprint, sshRemoteHost, "chmod -R a+rx", filepath.Join(destinationDirectory, filepath.Base(sourceFilepath)))
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrap(err, string(output))
	}

	return nil
}

// domainForIPAddr returns a domain name that publicly resolves to the given IP address.
func domainForIPAddr(ip net.IP) string {
	// xip.io is a public DNS that resolves an IP subdomain into the given IP address.
	return ip.String() + ".xip.io"
}
