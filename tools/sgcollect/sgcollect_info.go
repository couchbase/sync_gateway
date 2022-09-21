package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/rest"
	"github.com/google/uuid"
	"gopkg.in/alecthomas/kingpin.v2"
)

type LogRedactionLevel string

const (
	RedactNone    LogRedactionLevel = "none"
	RedactPartial                   = "partial"
)

// PasswordString is a string with marshallers that avoid accidentally printing it. It also makes it harder to accidentally
// pass to callers that won't know how to properly handle it.
type PasswordString string

func (p PasswordString) GoString() string {
	return strings.Repeat("*", len(p))
}

func (p PasswordString) MarshalText() ([]byte, error) {
	return bytes.Repeat([]byte("*"), len(p)), nil
}

type SGCollectOptions struct {
	OutputPath            string
	RootDir               string
	Verbosity             int
	ProductOnly           bool
	DumpUtilities         bool
	LogRedactionLevel     LogRedactionLevel
	LogRedactionSalt      PasswordString
	SyncGatewayURL        *url.URL
	SyncGatewayConfig     string
	SyncGatewayExecutable string
	SyncGatewayUsername   string
	SyncGatewayPassword   PasswordString
	HTTPTimeout           time.Duration
	TmpDir                string
}

func (opts *SGCollectOptions) ParseCommandLine(args []string) error {
	app := kingpin.New("sgcollect_info", "")
	app.Flag("root-dir", "root directory").StringVar(&opts.RootDir)
	app.Flag("verbosity", "").CounterVar(&opts.Verbosity)
	app.Flag("product_only", "").BoolVar(&opts.ProductOnly)
	app.Flag("dump_utilities", "").BoolVar(&opts.DumpUtilities)
	app.Flag("log-redaction-level", "").Default("none").EnumVar((*string)(&opts.LogRedactionLevel), "none", "partial")
	app.Flag("log-redaction-salt", "").Default(uuid.New().String()).StringVar((*string)(&opts.LogRedactionSalt))
	app.Flag("sync-gateway-url", "").URLVar(&opts.SyncGatewayURL)
	app.Flag("sync-gateway-username", "").StringVar(&opts.SyncGatewayUsername)
	app.Flag("sync-gateway-password", "").StringVar((*string)(&opts.SyncGatewayPassword))
	app.Flag("sync-gateway-config", "").ExistingFileVar(&opts.SyncGatewayConfig)
	app.Flag("sync-gateway-executable", "").ExistingFileVar(&opts.SyncGatewayExecutable)
	app.Flag("http-timeout", "").Default("30s").DurationVar(&opts.HTTPTimeout)
	app.Flag("tmp-dir", "").ExistingDirVar(&opts.TmpDir)
	app.Arg("path", "path to collect diagnostics into").Required().StringVar(&opts.OutputPath)
	_, err := app.Parse(args)
	return err
}

var (
	httpClient     *http.Client
	httpClientInit sync.Once
)

func getHTTPClient(opts *SGCollectOptions) *http.Client {
	httpClientInit.Do(func() {
		httpClient = &http.Client{
			Timeout: opts.HTTPTimeout,
		}
	})
	return httpClient
}

func getJSONOverHTTP(url string, opts *SGCollectOptions, result any) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to build HTTP request: %w", err)
	}
	req.SetBasicAuth(opts.SyncGatewayUsername, string(opts.SyncGatewayPassword))

	res, err := getHTTPClient(opts).Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute HTTP request: %w", err)
	}
	defer res.Body.Close()

	err = json.NewDecoder(res.Body).Decode(result)
	if err != nil {
		return fmt.Errorf("failed to decode response body: %w", err)
	}
	return nil
}

// determineSGURL attempts to find the Sync Gateway admin interface URL, starting with the one given in the options, then
// a default if one is not specified.
// Returns true if the URL is valid and reachable.
func determineSGURL(opts *SGCollectOptions) (*url.URL, bool) {
	sgURL := opts.SyncGatewayURL
	if sgURL == nil {
		sgURL, _ = url.Parse("http://127.0.0.1:4985")
	}
	log.Printf("Trying Sync Gateway URL: %s", sgURL)

	var root rest.DatabaseRoot
	err := getJSONOverHTTP(sgURL.String(), opts, &root)
	if err == nil {
		return sgURL, true
	}
	log.Printf("Failed to communicate with %s: %v", sgURL, err)

	// try HTTPS instead
	httpsURL := *sgURL
	httpsURL.Scheme = "https"
	log.Printf("Trying Sync Gateway URL: %s", httpsURL.String())
	err = getJSONOverHTTP(httpsURL.String(), opts, &root)
	if err == nil {
		return &httpsURL, true
	}
	log.Printf("Failed to communicate with %s: %v", httpsURL.String(), err)

	return sgURL, false
}

func findSGBinaryAndConfigsFromExpvars(sgURL *url.URL, opts *SGCollectOptions) (string, string, bool) {
	// Get path to sg binary (reliable) and config (not reliable)
	var expvars struct {
		CmdLine []string `json:"cmdline"`
	}
	err := getJSONOverHTTP(sgURL.String()+"/_expvar", opts, &expvars)
	if err != nil {
		log.Printf("findSGBinaryAndConfigsFromExpvars: Failed to get SG expvars: %v", err)
	}

	if len(expvars.CmdLine) == 0 {
		return "", "", false
	}

	binary := expvars.CmdLine[0]
	var config string
	for _, arg := range expvars.CmdLine[1:] {
		if strings.HasSuffix(arg, ".json") {
			config = arg
			break
		}
	}
	return binary, config, config != ""
}

var sgBinPaths = [...]string{
	"/opt/couchbase-sync-gateway/bin/sync_gateway",
	`C:\Program Files (x86)\Couchbase\sync_gateway.exe`,
	`C:\Program Files\Couchbase\Sync Gateway\sync_gateway.exe`,
}

var bootstrapConfigLocations = [...]string{
	"/home/sync_gateway/sync_gateway.json",
	"/opt/couchbase-sync-gateway/etc/sync_gateway.json",
	"/opt/sync_gateway/etc/sync_gateway.json",
	"/etc/sync_gateway/sync_gateway.json",
	`C:\Program Files (x86)\Couchbase\serviceconfig.json`,
	`C:\Program Files\Couchbase\Sync Gateway\serviceconfig.json`,
}

func findSGBinaryAndConfigs(sgURL *url.URL, opts *SGCollectOptions) (string, string) {
	// If the user manually passed some in, use those.
	binary := opts.SyncGatewayExecutable
	config := opts.SyncGatewayConfig
	if binary != "" && config != "" {
		return binary, config
	}

	var ok bool
	binary, config, ok = findSGBinaryAndConfigsFromExpvars(sgURL, opts)
	if ok {
		return binary, config
	}

	for _, path := range sgBinPaths {
		if _, err := os.Stat(path); err == nil {
			binary = path
			break
		}
	}

	for _, path := range bootstrapConfigLocations {
		if _, err := os.Stat(path); err == nil {
			config = path
			break
		}
	}
	return binary, config
}

func main() {
	opts := &SGCollectOptions{}
	if err := opts.ParseCommandLine(os.Args[1:]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	sgURL, ok := determineSGURL(opts)
	if !ok {
		log.Println("Failed to communicate with Sync Gateway. Check that Sync Gateway is reachable.")
		log.Println("Will attempt to continue, but some information may be unavailable, which may make troubleshooting difficult.")
	}

	// Build path to zip directory, make sure it exists
	zipFilename := opts.OutputPath
	if !strings.HasSuffix(zipFilename, ".zip") {
		zipFilename += ".zip"
	}
	zipDir := filepath.Dir(zipFilename)
	_, err := os.Stat(zipDir)
	if err != nil {
		log.Fatalf("Failed to check if output directory (%s) is accesible: %v", zipDir, err)
	}

	//shouldRedact := opts.LogRedactionLevel != RedactNone
	//var uploadURL string
	//var redactedZipFilename string
	//if shouldRedact {
	//	redactedZipFilename = strings.TrimSuffix(zipFilename, ".zip") + "-redacted.zip"
	//	uploadURL = generateUploadURL(opts, redactedZipFilename)
	//} else {
	//	uploadURL = generateUploadURL(opts, zipFilename)
	//}

	var config rest.RunTimeServerConfigResponse
	err = getJSONOverHTTP(sgURL.String()+"/_config?include_runtime=true", opts, &config)
	if err != nil {
		log.Printf("Failed to get SG config. Some information might not be collected.")
	}

	tr, err := NewTaskRunner(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer tr.Finalize()

	tr.Run(new(SGCollectOptionsTask))

	for _, task := range makeOSTasks() {
		tr.Run(task)
	}

	tasks := makeSGTasks(sgURL, opts, config)
	for _, task := range tasks {
		tr.Run(task)
	}
}
