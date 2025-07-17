/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// Package main generates a CA certificate, node certificate for Couchbase Server, and client certificate for Sync Gateway.
// That certificate is uploaded to Couchbase Server.
package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	x509TestFlagSSHUsername = "SG_TEST_X509_SSH_USERNAME"
	x509SSHDefaultUsername  = "root"

	x509CACommonName = "SG Test Root CA"

	permissions = os.FileMode(0600) // Permissions for the generated certs/keys

)

type cmdArgs struct {
	outputDir   string
	connStr     string
	ipAddrs     string // comma-separated list of IPs to use in SAN field of node cert
	dnsNames    string // comma-separated list of DNS names to use in SAN field of node cert
	dockerName  string // name of the Docker container to copy certs into, if using Docker
	sshUsername string // SSH username to use for copying certs into Couchbase Server
	cbsMgmtURL  string // URL for the Couchbase Server management API
	cbsUsername string // Username for Couchbase Server management API
	cbsPassword string // Password for Couchbase Server management API
}

// saveX509Files creates temp files for the given certs/keys and returns the full filepaths for each.
func saveX509Files(dirName string, ca *caPair, node *nodePair, sg *sgPair) error {
	log.Printf("Writing x509 certs to %s", dirName)
	err := os.MkdirAll(dirName, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error creating output directory %s: %w", dirName, err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("error getting current working directory: %w", err)
	}
	caPEMFilepath := filepath.Join(cwd, dirName, "ca.pem")
	err = os.WriteFile(caPEMFilepath, ca.PEM.Bytes(), permissions)
	if err != nil {
		return fmt.Errorf("error writing CA PEM file: %w", err)
	}
	ca.PEMFilepath = caPEMFilepath

	chainPEMFilepath := filepath.Join(cwd, dirName, "chain.pem")
	err = os.WriteFile(chainPEMFilepath, node.PEM.Bytes(), permissions)
	if err != nil {
		return fmt.Errorf("error writing node PEM file: %w", err)
	}
	node.PEMFilepath = chainPEMFilepath
	pkeyKeyFilepath := filepath.Join(cwd, dirName, "pkey.key")
	err = os.WriteFile(pkeyKeyFilepath, node.Key.Bytes(), permissions)
	if err != nil {
		return fmt.Errorf("error writing node key file: %w", err)
	}
	node.KeyFilePath = pkeyKeyFilepath

	sgPEMFilepath := filepath.Join(cwd, dirName, "sg.pem")
	err = os.WriteFile(sgPEMFilepath, sg.PEM.Bytes(), permissions)
	if err != nil {
		return fmt.Errorf("error writing SG PEM file: %w", err)
	}
	sg.PEMFilepath = sgPEMFilepath
	sgKeyFilepath := filepath.Join(cwd, dirName, "sg.key")
	err = os.WriteFile(sgKeyFilepath, sg.Key.Bytes(), permissions)
	if err != nil {
		return fmt.Errorf("error writing SG key file: %w", err)
	}
	sg.KeyFilePath = sgKeyFilepath
	return nil
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

func generateX509CA() (*caPair, error) {
	log.Printf("Generating X.509 CA certificate...")
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	caCert := &x509.Certificate{
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		Subject:               pkix.Name{CommonName: x509CACommonName},

		// Make the CA valid between yesterday and 10 years
		// This cert will be kept around on the server until the test next run, and we don't want it expiring
		NotBefore: time.Now().Add(time.Hour * -24),
		NotAfter:  time.Now().Add(time.Hour * 24 * 365 * 10),

		SerialNumber: big.NewInt(time.Now().UnixNano()),
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, caCert, caCert, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, err
	}

	caPEM := new(bytes.Buffer)
	err = pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})
	if err != nil {
		return nil, err
	}

	return &caPair{
		PEM:  caCertType{caPEM},
		Cert: caCert,
		Key:  caPrivKey,
	}, nil
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
func generateX509SG(ca *caPair, couchbaseUsername string, expiry time.Time) (*sgPair, error) {
	c, k, err := generateChainCert(ca, chainCertOpts{isClientCert: true, commonName: couchbaseUsername, expiryDate: expiry})
	if err != nil {
		return nil, fmt.Errorf("error generating SG client cert: %w", err)
	}
	return &sgPair{
		PEM: sgCertType{c},
		Key: sgKeyType{k},
	}, nil
}

// generateX509Node is a convenience wrapper around generateChainCert for generating SG client certs.
func generateX509Node(ca *caPair, sanIPs []net.IP, sanDNSNames []string) (*nodePair, error) {
	c, k, err := generateChainCert(ca, chainCertOpts{isClientCert: false, commonName: "cbs-node.local", sanIPs: sanIPs, sanDNSNames: sanDNSNames, expiryDate: time.Now().Add(time.Hour * 24 * 365 * 10)})
	if err != nil {
		return nil, fmt.Errorf("error generating node cert: %w", err)
	}
	return &nodePair{
		PEM: nodeCertType{c},
		Key: nodeKeyType{k},
	}, nil
}

// generateChainCert will produce a client or server (CBS Node) certificate and key authorised by the given CA with the given options.
// Use generateX509Node or generateX509SG instead for ease of use.
func generateChainCert(ca *caPair, opts chainCertOpts) (chainCert, chainKey *bytes.Buffer, err error) {
	clientPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("error generating client private key: %w", err)
	}

	chainCertTempl := &x509.Certificate{
		DNSNames: opts.sanDNSNames,
		//IPAddresses: opts.sanIPs,
		Subject: pkix.Name{CommonName: opts.commonName},

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
	if err != nil {
		return nil, nil, fmt.Errorf("error creating client certificate: %w", err)
	}

	chainCert = new(bytes.Buffer)
	err = pem.Encode(chainCert, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: clientCertBytes,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error encoding client certificate: %w", err)
	}

	chainKey = new(bytes.Buffer)
	err = pem.Encode(chainKey, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(clientPrivateKey),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error encoding client private key: %w", err)
	}

	return chainCert, chainKey, nil
}

// x509SSHUsername returns the configured SSH username to provision X.509 certs to Couchbase Server
func x509SSHUsername() string {
	if u := os.Getenv(x509TestFlagSSHUsername); u != "" {
		return u
	}
	return x509SSHDefaultUsername
}

// loadCertsIntoCouchbaseServer will upload the given certs into Couchbase Server (via SSH and the REST API)
func loadCertsIntoCouchbaseServer(args *cmdArgs, ca *caPair, node *nodePair) error {
	log.Printf("Loading X.509 certs into Couchbase Server at %s", args.cbsMgmtURL)
	parsedURL, err := url.Parse(args.cbsMgmtURL)
	if err != nil {
		return fmt.Errorf("error parsing management URL %s: %w", args.cbsMgmtURL, err)
	}
	if parsedURL.Hostname() == "" {
		return fmt.Errorf("management URL %s does not have a hostname", args.cbsMgmtURL)
	}
	// Copy node cert and key via SSH
	sshRemoteHost := x509SSHUsername() + "@" + parsedURL.Hostname()
	log.Printf("Copying x509 node cert and key to Couchbase Server at via SSH as %s", sshRemoteHost)
	err = sshCopyFileAsExecutable(node.PEMFilepath, sshRemoteHost, "/opt/couchbase/var/lib/couchbase/inbox")
	if err != nil {
		return err
	}
	log.Printf("copied x509 node chain.pem to integration test server")

	err = sshCopyFileAsExecutable(node.KeyFilePath, sshRemoteHost, "/opt/couchbase/var/lib/couchbase/inbox")
	if err != nil {
		return err
	}
	log.Printf(
		"copied x509 node pkey.key to integration test server")

	return uploadCACertViaREST(args, ca)
}

// loadCertsIntoCouchbaseServer will upload the given certs into Couchbase Server (via SSH and the REST API)
func loadCertsIntoCouchbaseServerDocker(args *cmdArgs, ca *caPair, node *nodePair) error {
	destDir := "/opt/couchbase/var/lib/couchbase/inbox"
	for _, filename := range []string{node.PEMFilepath, node.KeyFilePath} {
		err := copyLocalFileIntoDocker(args.dockerName, filename, destDir)
		if err != nil {
			return err
		}
	}

	return uploadCACertViaDocker(args, ca.PEM.String())
}

// loadCertsIntoLocalCouchbaseServer will upload the given certs into Couchbase Server (via SSH and the REST API)
func loadCertsIntoLocalCouchbaseServer(args *cmdArgs, ca *caPair, node *nodePair) error {

	localMacOSCouchbaseServerInbox := os.Getenv("HOME") + "/Library/Application Support/Couchbase/var/lib/couchbase/inbox"

	// Copy node cert and key
	err := copyLocalFile(node.PEMFilepath, localMacOSCouchbaseServerInbox)
	if err != nil {
		return err
	}
	log.Printf("copied x509 node chain.pem to integration test server")

	err = copyLocalFile(node.KeyFilePath, localMacOSCouchbaseServerInbox)
	if err != nil {
		return err
	}
	log.Printf("copied x509 node pkey.key to integration test server")
	return uploadCACertViaREST(args, ca)
}

func uploadCACertViaDocker(args *cmdArgs, caCert string) error {
	err := runCmd("curl", "--user", args.cbsUsername+":"+args.cbsPassword, "-X", "POST", "-H", "Content-Type:application/json", "--fail-with-body", "--trace-ascii", "-", "http://127.0.0.1:8091/controller/uploadClusterCA", "-d", caCert)
	if err != nil {
		return err
	}
	err = runCmd("docker", "exec", args.dockerName, "curl", "--user", args.cbsUsername+":"+args.cbsPassword, "-X", "POST", "--fail-with-body", "http://127.0.0.1:8091/node/controller/reloadCertificate")
	if err != nil {
		return err
	}
	return enableX509ClientCertsInCouchbaseServer(args)
}

func uploadCACertViaREST(args *cmdArgs, ca *caPair) error {
	log.Printf("Uploading CA cert to Couchbase Server at %s", args.cbsMgmtURL)
	// Upload the CA cert via the REST API
	_, err := request(requestOptions{
		Method:   http.MethodPost,
		URL:      args.cbsMgmtURL + "/controller/uploadClusterCA",
		Body:     ca.PEM.String(),
		Username: args.cbsUsername,
		Password: args.cbsPassword,
	})
	if err != nil {
		return err
	}
	log.Printf("uploaded ca.pem to Couchbase Server")

	// Make CBS read the newly uploaded certs
	_, err = request(requestOptions{
		Method:   http.MethodPost,
		URL:      args.cbsMgmtURL + "/node/controller/reloadCertificate",
		Username: args.cbsUsername,
		Password: args.cbsPassword,
	})
	if err != nil {
		return err
	}
	log.Printf("triggered reload of certificates on Couchbase Server")
	return enableX509ClientCertsInCouchbaseServer(args)
}

// couchbaseNodeConfiguredHostname returns the Couchbase node name for the given URL.
func couchbaseNodeConfiguredHostname(output []byte) (string, error) {
	type respStruct struct {
		NodesExt []struct {
			ThisNode bool   `json:"thisNode"`
			Hostname string `json:"hostname"`
		} `json:"nodes"`
	}
	var respJSON respStruct
	err := json.Unmarshal(output, &respJSON)
	if err != nil {
		return "", err
	}

	for _, n := range respJSON.NodesExt {
		if n.ThisNode {
			return n.Hostname, nil
		}
	}

	return "", fmt.Errorf("couldn't find 'thisNode' in nodeServices list: %v", respJSON)
}

// assertHostnameMatch ensures the hostname using for the test server matches what Couchbase Server's node hostname is.
func assertHostnameMatch(args *cmdArgs) error {
	output, err := request(requestOptions{
		Method: http.MethodGet,
		URL:    args.cbsMgmtURL + "/pools/default",

		Username: args.cbsUsername,
		Password: args.cbsPassword,
	})

	if err != nil {
		return err
	}

	nodeHostname, err := couchbaseNodeConfiguredHostname(output)
	if err != nil {
		return err
	}
	serverURI, err := url.Parse(args.connStr)
	if err != nil {
		return err
	}
	if nodeHostname != serverURI.Host {
		return fmt.Errorf("connection string %s must be the same as the Couchbase hostname configured %s. To resolve, use `curl -X POST /node/controller/rename -d hostname=%s", serverURI, nodeHostname, serverURI.Host)
	}
	return nil
}

func enableX509ClientCertsInCouchbaseServer(args *cmdArgs) error {
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
	_, err := request(requestOptions{
		Method:   http.MethodPost,
		URL:      args.cbsMgmtURL + "/settings/clientCertAuth",
		Body:     clientAuthSettings.String(),
		Username: args.cbsUsername,
		Password: args.cbsPassword,
	})
	if err != nil {
		return err
	}
	log.Printf("enabled X.509 client certs in Couchbase Server")
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
	log.Printf("Running %s", arg)
	cmd := exec.Command(arg[0], arg[1:]...) //#nosec G204 // unsanitized input OK for test code
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing %s: %s (err %w", cmd, output, err)
	}
	log.Printf("%s", output)
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

	log.Printf("Copying %s -> %s/%s in container %s", sourceFilepath, destinationDirectory, sourceFilepath, containerName)
	// copy the file
	err = runCmd("docker", "cp", sourceFilepath, containerName+":"+destinationDirectory)
	if err != nil {
		return err
	}

	// make the file we just copied readable and executable (required for Couchbase server to use the certs)
	return runCmd("docker", "exec", containerName, "chmod", "-R", "a+rwx", filepath.Join(destinationDirectory, filepath.Base(sourceFilepath)))
}

func parseArgs(cmdline []string) (*cmdArgs, error) {
	args := cmdArgs{}
	fs := flag.NewFlagSet(cmdline[0], flag.ContinueOnError)
	fs.StringVar(&args.outputDir, "output-dir", "", "Output directory for the x509 certificates.")
	fs.StringVar(&args.ipAddrs, "ip-addresses", "", "IP Addresses for Couchbase Server.")
	fs.StringVar(&args.dnsNames, "dns-names", "", "DNS names for Couchbase Server.")
	fs.StringVar(&args.dockerName, "docker-name", "", "Docker container name to copy certs into.")
	fs.StringVar(&args.sshUsername, "ssh-username", "", "SSH username to use for copying certs into Couchbase Server, if server is remote.")
	fs.StringVar(&args.cbsMgmtURL, "couchbase-server-mgmt-url", "", "Couchbase Server management URL (e.g. http://localhost:8091).")
	fs.StringVar(&args.connStr, "couchbase-server-connection-string", "", "Couchbase Server connection string (e.g. couchbases://localhost")
	fs.StringVar(&args.cbsUsername, "couchbase-server-username", "", "Couchbase Server management username.")
	fs.StringVar(&args.cbsPassword, "couchbase-server-password", "", "Couchbase Server management password.")
	if err := fs.Parse(cmdline[1:]); err != nil {
		return nil, err
	}
	if args.outputDir == "" {
		return nil, fmt.Errorf("output directory must be specified with -output")
	}
	if args.cbsMgmtURL == "" {
		return nil, fmt.Errorf("CBS management URL must be specified with -couchbase-server-mgmt-url")
	}
	if args.connStr == "" {
		return nil, fmt.Errorf("CBS connection string must be specified with -couchbase-connection-string")
	}
	return &args, nil
}

func main() {
	args, err := parseArgs(os.Args)
	if err != nil {
		log.Fatalf("Error parsing arguments: %v", err)
	}

	/*
		err = assertHostnameMatch(args)
		if err != nil {
			log.Fatalf("%s", err)
		}
	*/
	ca, err := generateX509CA()
	if err != nil {
		log.Fatalf("Error generating CA: %v", err)
	}
	var sanIPs []net.IP
	for ip := range strings.SplitSeq(args.ipAddrs, ",") {
		sanIPs = append(sanIPs, net.ParseIP(ip))
	}
	var dnsNames []string
	for dns := range strings.SplitSeq(args.dnsNames, ",") {
		dnsNames = append(dnsNames, strings.TrimSpace(dns))
	}
	nodePair, err := generateX509Node(ca, sanIPs, dnsNames)
	if err != nil {
		log.Fatalf("Error generating node cert: %v", err)
	}
	sgPair, err := generateX509SG(ca, args.cbsUsername, time.Now().Add(time.Hour*24))
	if err != nil {
		log.Fatalf("Error generating SG cert: %v", err)
	}
	err = saveX509Files(args.outputDir, ca, nodePair, sgPair)
	if err != nil {
		log.Fatalf("Error saving X.509 files: %v", err)
	}
	log.Printf("Loading X.509 certs from %s %s\n", nodePair.PEMFilepath, nodePair.KeyFilePath)
	_, err = tls.LoadX509KeyPair(sgPair.PEMFilepath, sgPair.KeyFilePath)
	if err != nil {
		log.Fatalf("Error loading SG cert and key: %v, this is a problem generating the certifcates", err)
	}
	if args.dockerName != "" {
		err := loadCertsIntoCouchbaseServerDocker(args, ca, nodePair)
		if err != nil {
			log.Fatalf("Error loading certs into Couchbase Server Docker: %v", err)
		}
	} else if args.sshUsername != "" {
		err := loadCertsIntoCouchbaseServer(args, ca, nodePair)
		if err != nil {
			log.Fatalf("Error loading certs into Couchbase Server via ssh: %v", err)
		}
	} else {
		err := loadCertsIntoLocalCouchbaseServer(args, ca, nodePair)
		if err != nil {
			log.Fatalf("Error loading certs into local Couchbase Server: %v", err)
		}
	}
	log.Printf("Successfully generated and loaded X.509 certs into Couchbase Server")
	clusterSpec := base.CouchbaseClusterSpec{
		Server:     args.connStr,
		Certpath:   sgPair.PEMFilepath,
		Keypath:    sgPair.KeyFilePath,
		CACertPath: ca.PEMFilepath,
	}
	log.Printf("Couchbase Cluster Spec: %+v\n", clusterSpec)
	clusterSpecJSON, err := json.Marshal(clusterSpec)
	if err != nil {
		log.Fatalf("Error marshalling Couchbase Cluster Spec to JSON: %v", err)
	}
	specFilename := filepath.Join(args.outputDir, "couchbase_cluster_spec.json")
	err = os.WriteFile(specFilename, clusterSpecJSON, permissions)
	if err != nil {
		log.Fatalf("Error writing Couchbase Cluster Spec to file: %v", err)
	}
	log.Printf("Couchbase Cluster Spec written to %s", specFilename)
}

type requestOptions struct {
	Method   string
	URL      string
	Body     string
	Username string
	Password string
}

func request(opt requestOptions) ([]byte, error) {
	req, err := http.NewRequest(opt.Method, opt.URL, strings.NewReader(opt.Body))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(opt.Username, opt.Password)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request to %s: %w", opt.URL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	output, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: unexpected status code: %d, output: %s", opt.Method, opt.URL, resp.StatusCode, output)
	}
	return output, nil
}
