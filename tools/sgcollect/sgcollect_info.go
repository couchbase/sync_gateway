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
	"runtime"
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
	log.Printf("Trying Sync Gateway URL: %s", httpsURL)
	err = getJSONOverHTTP(httpsURL.String(), opts, &root)
	if err == nil {
		return &httpsURL, true
	}
	log.Printf("Failed to communicate with %s: %v", httpsURL, err)

	return sgURL, false
}

const defaultOutputFile = "sync_gateway.log"

type TaskRunner struct {
	tmpDir    string
	startTime time.Time
	files     map[string]*os.File
	opts      *SGCollectOptions
}

func NewTaskRunner(opts *SGCollectOptions) (*TaskRunner, error) {
	tr := &TaskRunner{
		startTime: time.Now(),
		files:     make(map[string]*os.File),
		opts:      opts,
	}
	var err error
	tr.tmpDir, err = os.MkdirTemp(opts.TmpDir, fmt.Sprintf("sgcollect_info-%s-*", tr.startTime.Format("2006-01-02T15:04:05Z07")))
	if err != nil {
		return nil, fmt.Errorf("could not use temporary dir: %w", err)
	}
	log.Printf("Using temporary directory %s", tr.tmpDir)
	err = tr.setupSGCollectLog()
	if err != nil {
		return nil, err
	}
	return tr, nil
}

func (tr *TaskRunner) Finalize() {
	for _, fd := range tr.files {
		err := fd.Close()
		if err != nil {
			log.Printf("Failed to close %s: %v", fd.Name(), err)
		}
	}
}

// setupSGCollectLog will redirect the standard library log package's output to both stderr and a log file in the temporary directory.
func (tr *TaskRunner) setupSGCollectLog() error {
	fd, err := tr.createFile("sgcollect_info.log")
	if err != nil {
		return fmt.Errorf("failed to create sgcollect_info.log: %w", err)
	}
	tr.files["sgcollect_info.log"] = fd
	log.SetOutput(io.MultiWriter(os.Stderr, fd))
	return nil
}

func (tr *TaskRunner) createFile(name string) (*os.File, error) {
	path := filepath.Join(tr.tmpDir, name)
	return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
}

func (tr *TaskRunner) writeHeader(w io.Writer, task SGCollectTask) error {
	separator := strings.Repeat("=", 78)
	// example:
	// ==============================================================================
	// Collect server status
	// *main.URLTask: http://127.0.0.1:4985/_status
	// ==============================================================================
	_, err := fmt.Fprintf(w, "%s\n%s\n%T: %s\n%s\n", separator, task.Name(), task, task.Header(), separator)
	return err
}

func (tr *TaskRunner) Run(task SGCollectTask) {
	// TODO: opportunity to parallelise here - one worker per output file
	if pt, ok := task.(PlatformTask); ok && !pt.ShouldRun(runtime.GOOS) {
		log.Printf("Skipping %q on %s.", task.Name(), runtime.GOOS)
		return
	}
	if _, ok := task.(PrivilegedTask); ok {
		uid := os.Getuid()
		if uid != -1 && uid != 0 {
			log.Printf("Skipping %q - requires root privileges.", task.Name())
			return
		}
	}
	outputFile := task.OutputFile()
	if outputFile == "" {
		outputFile = defaultOutputFile
	}
	fd, ok := tr.files[outputFile]
	if !ok {
		var err error
		fd, err = os.OpenFile(filepath.Join(tr.tmpDir, outputFile), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("FAILed to run %q - failed to create file: %v", task.Name(), err)
			return
		}
		tr.files[outputFile] = fd
	}

	if header := task.Header(); header != "" {
		err := tr.writeHeader(fd, task)
		if err != nil {
			log.Printf("FAILed to run %q - failed to write header: %v", task.Name(), err)
			return
		}
	}

	if st, ok := task.(SampledTask); ok {
		for i := 0; i < st.NumSamples(); i++ {
			err := task.Run(tr.opts, fd) // TODO redact
			if err != nil {
				log.Printf("FAILed to run %q [%s]: %v", task.Name(), task.Header(), err)
				_, _ = fmt.Fprintln(fd, err.Error())
				return
			}
			log.Printf("Taking sample %d of %q [%s] after %v seconds", i+2 /* starts at 0 */, task.Name(), task.Header(), st.Interval())
			time.Sleep(st.Interval())
		}
	} else {
		err := task.Run(tr.opts, fd) // TODO redact
		if err != nil {
			log.Printf("FAILed to run %q [%s]: %v", task.Name(), task.Header(), err)
			_, _ = fmt.Fprintln(fd, err.Error())
			return
		}
	}
	log.Printf("OK - %s [%s]", task.Name(), task.Header())
	_, err := fd.WriteString("\n")
	if err != nil {
		log.Printf("WARN %s [%s] - failed to write closing newline: %v", task.Name(), task.Header(), err)
	}
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

func makeCollectLogsTasks(opts *SGCollectOptions, config rest.RunTimeServerConfigResponse) (result []SGCollectTask) {
	var sgLogFiles = []string{
		"sg_error",
		"sg_warn",
		"sg_info",
		"sg_debug",
		"sg_stats",
		"sync_gateway_access",
		"sync_gateway_error",
	}
	const sgLogExtensionNotRotated = ".log"
	const sgLogExtensionRotated = ".log.gz"
	var sgLogDirectories = []string{
		"/home/sync_gateway/logs",
		"/var/log/sync_gateway",
		"/Users/sync_gateway/logs",
		`C:\Program Files (x86)\Couchbase\var\lib\couchbase\logs`,
		`C:\Program Files\Couchbase\var\lib\couchbase\logs`,
		`C:\Program Files\Couchbase\Sync Gateway\var\lib\couchbase\logs`,
	}

	// Also try getting the current path from the config, in case it's not one of the defaults
	if cfgPath := config.Logging.LogFilePath; cfgPath != "" {
		// This could be a relative path
		if !filepath.IsAbs(cfgPath) {
			cfgPath = filepath.Join(opts.RootDir, cfgPath)
		}
		sgLogDirectories = append(sgLogDirectories, config.Logging.LogFilePath)
	}

	// Check every combination of directory/file, grab everything we can
	for _, dir := range sgLogDirectories {
		// Bail out if the directory doesn't exist, avoids unnecessary checks
		_, err := os.Stat(dir)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				log.Printf("WARN: failed to stat %q: %v", dir, err)
			}
			continue
		}
		for _, file := range sgLogFiles {
			// Grab the rotated files first, that way they'll be in the right order when ungzipped
			rotated, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("%s-*%s", file, sgLogExtensionRotated)))
			if err != nil {
				log.Printf("WARN: failed to glob %s in %s: %v", file, dir, err)
			} else {
				for _, rotatedFile := range rotated {
					result = append(result, &GZipFileTask{
						name:       file + sgLogExtensionNotRotated,
						inputFile:  rotatedFile,
						outputFile: file + sgLogExtensionNotRotated,
					})
				}
			}
			result = append(result, &FileTask{
				name:       file + sgLogExtensionNotRotated,
				inputFile:  filepath.Join(dir, file+sgLogExtensionNotRotated),
				outputFile: file + sgLogExtensionNotRotated,
			})
		}
	}
	return result
}

func makeSGTasks(url *url.URL, opts *SGCollectOptions, config rest.RunTimeServerConfigResponse) (result []SGCollectTask) {
	binary, bootstrapConfigPath := findSGBinaryAndConfigs(url, opts)
	if binary != "" {
		result = append(result, &FileTask{
			name:       "Sync Gateway executable",
			inputFile:  binary,
			outputFile: "sync_gateway",
		})
	}
	if bootstrapConfigPath != "" {
		result = append(result, &FileTask{
			name:       "Sync Gateway bootstrapConfigPath",
			inputFile:  bootstrapConfigPath,
			outputFile: "sync_gateway.json",
		})
	}

	result = append(result, &URLTask{
		name:       "Sync Gateway expvars",
		url:        url.String() + "/_expvar",
		outputFile: "expvars.json",
	}, &URLTask{
		name: "Collect server bootstrapConfigPath",
		url:  url.String() + "/_config",
	}, &URLTask{
		name: "Collect runtime bootstrapConfigPath",
		url:  url.String() + "/_config?include_runtime=true",
	}, &URLTask{
		name: "Collect server status",
		url:  url.String() + "/_status",
	})
	result = append(result, makeCollectLogsTasks(opts, config)...)
	return
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

	tasks := makeSGTasks(sgURL, opts, config)
	for _, task := range tasks {
		tr.Run(task)
	}
}
