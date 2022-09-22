package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
	UploadHost            *url.URL
	UploadCustomer        string
	UploadTicketNumber    string
	UploadProxy           *url.URL
}

func (opts *SGCollectOptions) ParseCommandLine(args []string) error {
	app := kingpin.New("sgcollect_info", "")
	app.Flag("root-dir", "root directory").StringVar(&opts.RootDir)
	app.Flag("verbosity", "").CounterVar(&opts.Verbosity)
	app.Flag("log-redaction-level", "").Default("none").EnumVar((*string)(&opts.LogRedactionLevel), "none", "partial")
	app.Flag("log-redaction-salt", "").Default(uuid.New().String()).StringVar((*string)(&opts.LogRedactionSalt))
	app.Flag("sync-gateway-url", "").URLVar(&opts.SyncGatewayURL)
	app.Flag("sync-gateway-username", "").StringVar(&opts.SyncGatewayUsername)
	app.Flag("sync-gateway-password", "").StringVar((*string)(&opts.SyncGatewayPassword))
	app.Flag("sync-gateway-config", "").ExistingFileVar(&opts.SyncGatewayConfig)
	app.Flag("sync-gateway-executable", "").ExistingFileVar(&opts.SyncGatewayExecutable)
	app.Flag("http-timeout", "").Default("30s").DurationVar(&opts.HTTPTimeout)
	app.Flag("tmp-dir", "").ExistingDirVar(&opts.TmpDir)
	app.Flag("upload-host", "").URLVar(&opts.UploadHost)
	app.Flag("customer", "").StringVar(&opts.UploadCustomer)
	app.Flag("ticket", "").StringVar(&opts.UploadTicketNumber)
	app.Flag("upload-proxy", "").URLVar(&opts.UploadProxy)
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

	var root map[string]any
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
	"./sync_gateway",
}

var bootstrapConfigLocations = [...]string{
	"/home/sync_gateway/sync_gateway.json",
	"/opt/couchbase-sync-gateway/etc/sync_gateway.json",
	"/opt/sync_gateway/etc/sync_gateway.json",
	"/etc/sync_gateway/sync_gateway.json",
	`C:\Program Files (x86)\Couchbase\serviceconfig.json`,
	`C:\Program Files\Couchbase\Sync Gateway\serviceconfig.json`,
	"./sync_gateway.json",
}

func findSGBinaryAndConfigs(sgURL *url.URL, opts *SGCollectOptions) (string, string) {
	// If the user manually passed some in, use those.
	binary := opts.SyncGatewayExecutable
	config := opts.SyncGatewayConfig
	if binary != "" && config != "" {
		log.Printf("Using manually passed SG binary at %q and config at %q.", binary, config)
		return binary, config
	}

	var ok bool
	binary, config, ok = findSGBinaryAndConfigsFromExpvars(sgURL, opts)
	if ok {
		log.Printf("SG binary at %q and config at %q.", binary, config)
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
	log.Printf("SG binary at %q and config at %q.", binary, config)
	return binary, config
}

func main() {
	opts := &SGCollectOptions{}
	if err := opts.ParseCommandLine(os.Args[1:]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	tr, err := NewTaskRunner(opts)
	if err != nil {
		log.Fatal(err)
	}
	err = tr.SetupSGCollectLog()
	if err != nil {
		log.Printf("Failed to set up sgcollect_info.log: %v. Will continue.", err)
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
	_, err = os.Stat(zipDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Fatalf("Output directory %s does not exist.", zipDir)
		} else {
			log.Fatalf("Failed to check if output directory (%s) is accesible: %v", zipDir, err)
		}
	}

	shouldRedact := opts.LogRedactionLevel != RedactNone
	var redactedZipFilename string
	var uploadFilename string
	if shouldRedact {
		redactedZipFilename = strings.TrimSuffix(zipFilename, ".zip") + "-redacted.zip"
		uploadFilename = redactedZipFilename
	} else {
		uploadFilename = zipFilename
	}

	var config serverConfig
	err = getJSONOverHTTP(sgURL.String()+"/_config?include_runtime=true", opts, &config)
	if err != nil {
		log.Printf("Failed to get SG config. Some information might not be collected.")
	}

	tr.Run(new(SGCollectOptionsTask))

	for _, task := range makeOSTasks() {
		tr.Run(task)
	}

	tasks := makeSGTasks(sgURL, opts, config)
	for _, task := range tasks {
		tr.Run(task)
	}

	tr.Finalize()
	log.Printf("Writing unredacted logs to %s", zipFilename)
	hostname, _ := os.Hostname()
	prefix := fmt.Sprintf("sgcollect_info_%s_%s", hostname, time.Now().Format("20060102-150405"))
	err = tr.ZipResults(zipFilename, prefix, io.Copy)
	if err != nil {
		log.Printf("WARNING: failed to produce output file %s: %v", zipFilename, err)
	}
	if shouldRedact {
		log.Printf("Writing redacted logs to %s", redactedZipFilename)
		err = tr.ZipResults(redactedZipFilename, prefix, RedactCopier(opts))
		if err != nil {
			log.Printf("WARNING: failed to produce output file %s: %v", redactedZipFilename, err)
		}
	}

	if opts.UploadHost != nil && opts.UploadCustomer != "" {
		uploadURL := *opts.UploadHost
		uploadURL.Path += fmt.Sprintf("/%s/", opts.UploadCustomer)
		if opts.UploadTicketNumber != "" {
			uploadURL.Path += fmt.Sprintf("%s/", opts.UploadTicketNumber)
		}
		uploadURL.Path += filepath.Base(uploadFilename)
		log.Printf("Uploading archive to %s...", uploadURL.String())

		fd, err := os.Open(uploadFilename)
		if err != nil {
			log.Fatalf("Failed to prepare file for upload: %v", err)
		}
		defer fd.Close()
		stat, err := fd.Stat()
		if err != nil {
			log.Fatalf("Failed to stat upload file: %v", err)
		}

		req, err := http.NewRequest(http.MethodPut, uploadURL.String(), fd)
		if err != nil {
			log.Fatalf("Failed to prepare upload request: %v", err)
		}
		req.Header.Set("Content-Type", "application/zip")
		req.ContentLength = stat.Size()

		var proxy func(*http.Request) (*url.URL, error)
		if opts.UploadProxy != nil {
			proxy = http.ProxyURL(opts.UploadProxy)
		} else {
			proxy = http.ProxyFromEnvironment
		}
		httpClient := &http.Client{
			Transport: &http.Transport{
				Proxy: proxy,
			},
		}
		res, err := httpClient.Do(req)
		if err != nil {
			log.Fatalf("Failed to upload logs: %v", err)
		}
		defer res.Body.Close()
		if res.StatusCode != 200 {
			log.Printf("WARN: upload gave unexpected status %s", res.Status)
			body, _ := io.ReadAll(res.Body)
			log.Println(string(body))
		}
	}

	log.Println("Done.")
}
