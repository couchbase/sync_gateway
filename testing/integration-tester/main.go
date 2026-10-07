// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// integration-tester is a Go port of jenkins-integration-build.sh.
// It orchestrates rosmar and Couchbase-Server integration test runs.
//
// Required environment variables (or use -m for master-mode defaults):
//
//	COUCHBASE_SERVER_PROTOCOL, COUCHBASE_SERVER_VERSION, GSI,
//	TARGET_TEST, TARGET_PACKAGE, TLS_SKIP_VERIFY, SG_EDITION
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"go.uber.org/zap"
)

const (
	defaultPackageTimeout      = "45m"
	defaultMaxParallelPackages = 5
	sgModule                   = "github.com/couchbase/sync_gateway"
	allPackages                = "..."

	// gotestsum flag values shared across all invocations.
	junitTestcaseClassname = "--junitfile-testcase-classname=relative"
	gotestsumFormat        = "--format=standard-verbose"
)

var (
	logger   *zap.SugaredLogger
	extraEnv = map[string]string{}
	nonFatal errCollector
	// filterOutput drops test output lines that don't match outputPattern from the console, even with -v.
	filterOutput bool

	// Package-level compiled regexps for gotestsum output parsing.
	outputPattern = regexp.MustCompile(`--- (FAIL|PASS|SKIP):|github\.com/couchbase/sync_gateway(/.+)?\t|TEST: |panic: |Assertion failed: `)
	failPattern   = regexp.MustCompile(`--- FAIL: (\S+)`)
	// packagePattern matches go test's per-package result lines, e.g. "ok  \tgithub.com/couchbase/sync_gateway/db".
	packagePattern = regexp.MustCompile(`github\.com/couchbase/sync_gateway(/.+)?\t`)
)

// errCollector accumulates non-fatal errors from across the run and reports them
// all at error level at the end, so individual failures don't get buried.
type errCollector struct {
	errs []error
	mu   sync.Mutex
}

func (c *errCollector) add(err error) {
	if err == nil {
		return
	}
	logger.Error(err)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
}

func (c *errCollector) report() {
	c.mu.Lock()
	errs := c.errs
	c.mu.Unlock()
	if len(errs) == 0 {
		return
	}
	logger.Errorf("%d non-fatal error(s) during run:", len(errs))
	for _, err := range errs {
		logger.Errorf("  %v", err)
	}
}

type Edition string

const (
	EditionEE Edition = "EE"
	EditionCE Edition = "CE"
)

func (e *Edition) UnmarshalText(text []byte) error {
	switch Edition(text) {
	case EditionEE, EditionCE:
		*e = Edition(text)
		return nil
	default:
		return fmt.Errorf("must be EE or CE, got %q", string(text))
	}
}

type Protocol string

const (
	ProtocolCouchbase  Protocol = "couchbase"
	ProtocolCouchbases Protocol = "couchbases"
)

func (p *Protocol) UnmarshalText(text []byte) error {
	switch Protocol(text) {
	case ProtocolCouchbase, ProtocolCouchbases:
		*p = Protocol(text)
		return nil
	default:
		return fmt.Errorf("must be couchbase or couchbases, got %q", string(text))
	}
}

// config holds all runtime configuration parsed from environment variables.
// Fields are ordered to minimise struct padding (strings, then int, then bools).
type config struct {
	// Required
	CouchbaseServerProtocol Protocol `env:"COUCHBASE_SERVER_PROTOCOL,required"`
	CouchbaseServerVersion  string   `env:"COUCHBASE_SERVER_VERSION,required"`
	TargetTest              string   `env:"TARGET_TEST,required"`
	TargetPackage           string   `env:"TARGET_PACKAGE,required"`
	SGEdition               Edition  `env:"SG_EDITION,required"`
	// Optional strings
	PackageTimeout string `env:"PACKAGE_TIMEOUT"`
	// Optional ints
	RunCount            int `env:"RUN_COUNT" envDefault:"1"`
	MaxParallelPackages int `env:"MAX_PARALLEL_PACKAGES"`
	// Required bools
	GSI           bool `env:"GSI,required"`
	TLSSkipVerify bool `env:"TLS_SKIP_VERIFY,required"`
	// Optional bools
	RunWalrus         bool `env:"RUN_WALRUS"`
	DetectRaces       bool `env:"DETECT_RACES"`
	FailFast          bool `env:"FAIL_FAST"`
	MultiNode         bool `env:"MULTI_NODE"`
	TestDebug         bool `env:"TEST_DEBUG"`
	DisableRevCache   bool `env:"DISABLE_REV_CACHE"`
	SGTestX509        bool `env:"SG_TEST_X509"`
	SGCBCollectAlways bool `env:"SG_CBCOLLECT_ALWAYS"`
	SGTestBucketDebug bool `env:"SG_TEST_BUCKET_POOL_DEBUG"`
}

// goTestArgs holds the arguments for a single gotestsum invocation.
type goTestArgs struct {
	junitFile        string   // path to the JUnit XML output file (--junitfile)
	junitProjectName string   // project name embedded in the JUnit report (--junitfile-project-name)
	coverProfile     string   // path for the go test -coverprofile output file
	label            string   // prefix for log lines, e.g. "[rest]"; empty = no prefix
	summaryOnly      bool     // with -filter, print only failures, panics and package results to the console
	edition          Edition  // CE or EE; controls which build tags are passed to go test
	packages         []pkg    // packages to test; cmdArgs converts each to a relative path via goTestPath
	goTestFlags      []string // additional flags forwarded verbatim to go test (e.g. -v, -run, -timeout)
}

// cmdArgs builds the full gotestsum argument list from the struct fields.
func (a goTestArgs) cmdArgs() []string {
	tags := "cb_sg_devmode"
	if a.edition == EditionEE {
		tags += ",cb_sg_enterprise"
	}
	args := make([]string, 0, 8+len(a.goTestFlags)+len(a.packages))
	args = append(args,
		"--junitfile="+a.junitFile,
		"--junitfile-project-name="+a.junitProjectName,
		junitTestcaseClassname,
		gotestsumFormat,
		"--",
		"-coverprofile="+a.coverProfile,
		"-coverpkg="+sgModule+"/...",
		"-tags="+tags,
	)
	args = append(args, a.goTestFlags...)
	for _, p := range a.packages {
		args = append(args, p.goTestPath())
	}
	return args
}

// mergeEnv returns os.Environ() with extraEnv applied on top, then any additional
// key=value pairs in extras applied last. Later entries win on duplicate keys.
func mergeEnv(extras ...string) []string {
	overrides := make(map[string]string, len(extraEnv)+len(extras))
	maps.Copy(overrides, extraEnv)
	for _, e := range extras {
		k, v, _ := strings.Cut(e, "=")
		overrides[k] = v
	}

	base := os.Environ()
	result := make([]string, 0, len(base)+len(overrides))
	for _, e := range base {
		k, _, _ := strings.Cut(e, "=")
		if _, ok := overrides[k]; !ok {
			result = append(result, e)
		}
	}
	for k, v := range overrides {
		result = append(result, k+"="+v)
	}
	return result
}

func main() {
	os.Exit(run())
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage: integration-tester [flags]

Flags:
`)
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, `
Environment variables:

  Required:
    COUCHBASE_SERVER_PROTOCOL  Couchbase Server connection protocol (couchbase|couchbases)
    COUCHBASE_SERVER_VERSION   Couchbase Server version to provision (e.g. enterprise-7.6.6)
    GSI                        Use GSI (global secondary index) for queries (true|false)
    SG_EDITION                 Sync Gateway edition to test (EE|CE)
    TARGET_PACKAGE             Comma-separated package(s) to test (e.g. rest,db or ...)
    TARGET_TEST                Test name filter passed to -run, or ALL to run everything
    TLS_SKIP_VERIFY            Skip TLS certificate verification (true|false)

  Optional:
    DISABLE_REV_CACHE          Disable the revision cache during tests (default: false)
    DETECT_RACES               Enable the Go race detector (default: false)
    FAIL_FAST                  Stop on the first test failure (default: false)
    MAX_PARALLEL_PACKAGES      Maximum packages tested simultaneously against CBS (default: %d)
    MULTI_NODE                 Provision a 3-node Couchbase cluster instead of 1 (default: false)
    PACKAGE_TIMEOUT            Per-package test timeout passed to -test.timeout (default: %s)
    RUN_COUNT                  Number of times to repeat each test run (default: 1)
    RUN_WALRUS                 Also run rosmar (in-memory) tests before CBS tests (default: false)
    SG_CBCOLLECT_ALWAYS        Always collect cbcollect_info after each package run (default: false)
    SG_TEST_BUCKET_POOL_DEBUG  Enable bucket pool debug logging in tests (default: false)
    SG_TEST_X509               Use X.509 certificate auth (requires couchbases://) (default: false)
    TEST_DEBUG                 Set test log level to debug and enable bucket pool debug (default: false)
`, defaultMaxParallelPackages, defaultPackageTimeout)
}

func run() int {
	masterMode := flag.Bool("m", false, "Run in automated master integration mode")
	verbose := flag.Bool("v", false, "Enable verbose/debug logging")
	flag.BoolVar(&filterOutput, "filter", false, "Only print test output lines matching the summary pattern, even with -v")
	flag.Usage = printUsage
	flag.Parse()

	initLogger(*verbose)
	defer func() { _ = logger.Sync() }()

	var cfg config
	if *masterMode {
		logger.Info("Running in automated master integration mode")
		cfg = config{
			TargetPackage:           allPackages,
			TargetTest:              "ALL",
			SGEdition:               EditionEE,
			RunCount:                1,
			MaxParallelPackages:     defaultMaxParallelPackages,
			CouchbaseServerProtocol: ProtocolCouchbase,
			CouchbaseServerVersion:  "enterprise-7.6.6",
			SGTestBucketDebug:       true,
			GSI:                     true,
			TLSSkipVerify:           false,
			SGCBCollectAlways:       false,
			RunWalrus:               true,
			DetectRaces:             false,
		}
	} else {
		if err := env.Parse(&cfg); err != nil {
			logger.Fatalf("parse environment: %v", err)
		}
		if cfg.MaxParallelPackages <= 0 {
			cfg.MaxParallelPackages = defaultMaxParallelPackages
		}
	}

	hash := mustOutput("git", "rev-parse", "HEAD")
	logger.Infof("Sync Gateway git commit hash: %s", strings.TrimSpace(hash))

	goVersionShort := strings.TrimSpace(mustOutput("go", "list", "-m", "-f", "{{.GoVersion}}"))
	goVersion := "go" + goVersionShort
	logger.Infof("Sync Gateway go.mod version is %s", goVersion)

	mustRunRetry("go", "install", "golang.org/dl/"+goVersion+"@latest")

	homeDir, err := os.UserHomeDir()
	if err != nil {
		logger.Fatalf("get home dir: %v", err)
	}
	goVersionBin := filepath.Join(homeDir, "go", "bin", goVersion)
	mustRunRetry(goVersionBin, "download")

	goRoot := strings.TrimSpace(mustOutput(goVersionBin, "env", "GOROOT"))
	extraEnv["PATH"] = filepath.Join(goRoot, "bin") + ":" + os.Getenv("PATH")

	// Sync the current process PATH so exec.Command finds the go toolchain installed above.
	// extraEnv only affects child process environments, not exec.LookPath.
	if err := os.Setenv("PATH", extraEnv["PATH"]); err != nil {
		logger.Fatalf("update PATH: %v", err)
	}

	// Download modules up front, where a flaky module proxy is retried, rather than part-way through a test run.
	logger.Info("Downloading module dependencies...")
	mustRunRetry("go", "mod", "download")
	mustRunRetry("go", "-C", cbdinoToolsDir, "mod", "download")

	// gotestsum is pinned by a tool directive in go.mod. Build it once so test runs exec it directly.
	toolBinDir, err := os.MkdirTemp("", "integration-tester-bin-")
	if err != nil {
		logger.Fatalf("create tool bin dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(toolBinDir) }()
	mustRunRetry("go", "build", "-o", filepath.Join(toolBinDir, "gotestsum"), "gotest.tools/gotestsum")
	extraEnv["PATH"] = toolBinDir + ":" + extraEnv["PATH"]
	if err := os.Setenv("PATH", extraEnv["PATH"]); err != nil {
		logger.Fatalf("update PATH: %v", err)
	}

	if cfg.SGTestX509 && cfg.CouchbaseServerProtocol != ProtocolCouchbases {
		logger.Fatal("Setting SG_TEST_X509 requires using couchbases:// protocol, aborting integration tests")
	}

	goTestFlags := []string{"-v", "-p", "1", fmt.Sprintf("-count=%d", cfg.RunCount)}
	intLogFileName := "verbose_int"

	if cfg.TestDebug {
		extraEnv["SG_TEST_LOG_LEVEL"] = "debug"
		extraEnv["SG_TEST_BUCKET_POOL_DEBUG"] = "true"
	}

	if cfg.DisableRevCache {
		extraEnv["SG_TEST_DISABLE_REV_CACHE"] = "true"
	}

	if cfg.TargetTest != "ALL" {
		goTestFlags = append(goTestFlags, "-run", cfg.TargetTest)
	}

	if cfg.PackageTimeout != "" {
		goTestFlags = append(goTestFlags, "-test.timeout="+cfg.PackageTimeout)
	} else {
		logger.Infof("Defaulting package timeout to %s", defaultPackageTimeout)
		goTestFlags = append(goTestFlags, "-test.timeout="+defaultPackageTimeout)
	}

	if cfg.DetectRaces {
		goTestFlags = append(goTestFlags, "-race")
	}

	if cfg.FailFast {
		goTestFlags = append(goTestFlags, "-failfast")
	}

	sgPkgs := buildPackageList(cfg.TargetPackage)

	if cfg.RunWalrus {
		runRosmarTests(goTestFlags, sgPkgs)
	}

	// Set common CBS test environment variables shared across all package invocations.
	if cfg.MultiNode {
		extraEnv["SG_TEST_BUCKET_NUM_REPLICAS"] = "1"
	}
	extraEnv["SG_TEST_USE_GSI"] = fmt.Sprintf("%v", cfg.GSI)
	extraEnv["SG_TEST_BACKING_STORE"] = "Couchbase"
	extraEnv["SG_TEST_TLS_SKIP_VERIFY"] = fmt.Sprintf("%v", cfg.TLSSkipVerify)

	testFailed, logFiles := runIntegrationTestsParallel(cfg.SGEdition, goTestFlags, sgPkgs, cfg.CouchbaseServerVersion, cfg.CouchbaseServerProtocol, cfg.MultiNode, cfg.MaxParallelPackages, intLogFileName, cfg.SGCBCollectAlways, cfg.FailFast)

	// Classnames only need telling apart from the rosmar results when both are reported.
	classnamePrefix := ""
	if cfg.RunWalrus {
		classnamePrefix = "integration-" + string(cfg.SGEdition) + "-"
	}
	if err := prefixTestcaseClassnames("integration.xml", intLogFileName+".xml", classnamePrefix); err != nil {
		logger.Fatalf("prefix integration XML classnames: %v", err)
	}

	nonFatal.report()

	if testFailed {
		if filesContainsAny(logFiles, "FAIL:") {
			return 50
		}
		return 1
	}
	return 0
}

// runGotestsum runs gotestsum with the given args, writing all output to logFile and
// emitting matched lines through the logger. envExtras are additional key=value env
// overrides for this specific invocation. Returns the names of failed tests.
// If ctx is cancelled, returns nil, nil (cancellation is not treated as a test failure).
func runGotestsum(ctx context.Context, args goTestArgs, logFile string, envExtras ...string) ([]string, error) {
	outFile, err := os.Create(logFile) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("create log file %q: %w", logFile, err)
	}
	defer func() { _ = outFile.Close() }()

	pr, pw := io.Pipe()
	cmd := exec.CommandContext(ctx, "gotestsum", args.cmdArgs()...) //nolint:gosec
	cmd.Stdout = pw
	cmd.Stderr = pw
	cmd.Env = mergeEnv(envExtras...)
	prefix := labelPrefix(args.label)
	printCommand(prefix, cmd, envExtras...)
	logger.Debugf("%s  out  %s", prefix, logFile)
	logger.Debugf("%s  xml  %s", prefix, args.junitFile)
	logger.Debugf("%s  cov  %s", prefix, args.coverProfile)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start gotestsum: %w", err)
	}

	var failedTests []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		scanner := bufio.NewScanner(pr)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			_, _ = fmt.Fprintln(outFile, line)
			consoleLine := prefix + shortenTimestamps(line)
			m := failPattern.FindStringSubmatch(line)
			switch {
			case m != nil || strings.Contains(line, "panic: "):
				logger.Warn(consoleLine)
			case filterOutput && args.summaryOnly:
				if packagePattern.MatchString(line) {
					logger.Info(consoleLine)
				}
			case outputPattern.MatchString(line):
				logger.Info(consoleLine)
			case !filterOutput:
				logger.Debug(consoleLine)
			}
			if m != nil {
				failedTests = append(failedTests, m[1])
			}
		}
		if err := scanner.Err(); err != nil {
			nonFatal.add(fmt.Errorf("gotestsum output scanner: %w", err))
		}
	}()

	runErr := cmd.Wait()
	_ = pw.Close()
	<-done
	removeEmptyCoverProfile(args.coverProfile)

	if runErr != nil {
		if ctx.Err() != nil {
			return nil, nil
		}
		return failedTests, runErr
	}
	return nil, nil
}

// removeEmptyCoverProfile deletes a coverage profile that has no coverage blocks, which go test
// leaves behind (empty or header-only) when the test binary fails to build or is killed.
func removeEmptyCoverProfile(path string) {
	f, err := os.Open(path) //nolint:gosec
	if err != nil {
		return
	}
	scanner := bufio.NewScanner(f)
	hasData := false
	for !hasData && scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		hasData = line != "" && !strings.HasPrefix(line, "mode:")
	}
	_ = f.Close()
	if err := scanner.Err(); err != nil {
		nonFatal.add(fmt.Errorf("read coverage profile %q: %w", path, err))
		return
	}
	if hasData {
		return
	}
	logger.Debugf("removing empty coverage profile %s", path)
	if err := os.Remove(path); err != nil {
		nonFatal.add(fmt.Errorf("remove empty coverage profile %q: %w", path, err))
	}
}

// runRosmarTests runs EE and CE rosmar (in-memory) tests, ignoring failures (set +e equivalent).
func runRosmarTests(baseFlags []string, pkgs []pkg) {
	for _, ed := range []Edition{EditionEE, EditionCE} {
		logger.Infof("Running rosmar (%s) tests...", ed)
		suffix := strings.ToLower(string(ed))
		args := goTestArgs{
			junitFile:        "rosmar-" + suffix + ".xml",
			junitProjectName: "rosmar-" + string(ed),
			coverProfile:     "coverage_rosmar_" + suffix + ".out",
			label:            "rosmar-" + suffix,
			summaryOnly:      true,
			edition:          ed,
			packages:         pkgs,
			goTestFlags:      baseFlags,
		}
		if _, err := runGotestsum(context.Background(), args, "verbose_unit_"+suffix+".out"); err != nil {
			nonFatal.add(fmt.Errorf("rosmar gotestsum (%s): %w", ed, err))
		}
		xmlOut := "verbose_unit_" + suffix + ".xml"
		if err := prefixTestcaseClassnames("rosmar-"+suffix+".xml", xmlOut, "rosmar-"+string(ed)+"-"); err != nil {
			nonFatal.add(fmt.Errorf("rosmar classname prefix (%s): %w", ed, err))
		}
	}
}

type result struct {
	path        pkg
	logFile     string
	xmlFile     string
	failedTests []string
	duration    time.Duration
	failed      bool
}

// runIntegrationTestsParallel runs CBS integration tests for each package concurrently,
// each with its own cbdinocluster instance, capped at maxParallel simultaneous runs.
// Per-package log and XML files are named after the package. XML files are merged into
// integration.xml. The first package failure cancels all remaining runs.
// Returns (anyFailed, logFilePaths).
func runIntegrationTestsParallel(edition Edition, flags []string, pkgs []pkg, serverVersion string, protocol Protocol, multiNode bool, maxParallel int, logFileName string, cbcollectAlways bool, failFast bool) (bool, []string) {
	results := make([]result, len(pkgs))

	// Initialize cbdinocluster once; each package invocation allocates its own cluster.
	if err := initCbdinocluster(); err != nil {
		logger.Fatal(err)
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxParallel)

	for i, p := range pkgs {
		wg.Go(func() {
			select {
			case <-ctx.Done():
				results[i] = result{path: p}
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()

			select {
			case <-ctx.Done():
				results[i] = result{path: p}
				return
			default:
			}

			clusterID, connStr, err := allocateCluster(p.shortName(), serverVersion, protocol, multiNode)
			if err != nil {
				nonFatal.add(fmt.Errorf("allocate cluster for %s: %w", p, err))
				results[i] = result{path: p, failed: true}
				cancel(fmt.Errorf("package %s: cluster allocation failed", p))
				return
			}
			defer deallocateCluster(p.shortName(), clusterID)

			dockerName, err := kvDockerName(p.shortName(), clusterID)
			if err != nil {
				nonFatal.add(fmt.Errorf("get docker name for %s: %w", p, err))
				// dockerName is ""; tests proceed without SG_TEST_COUCHBASE_SERVER_DOCKER_NAME
			}

			logger.Infof("[%s] starting integration tests", p.shortName())
			results[i] = runPackageIntegrationTests(ctx, edition, flags, p, logFileName, connStr, dockerName)
			logger.Infof("[%s] done in %s (failed=%v)", p.shortName(), results[i].duration.Round(time.Second), results[i].failed)
			if results[i].failed && failFast {
				cancel(fmt.Errorf("package %s failed", p))
			}

			// Collect server diagnostics before the cluster is deallocated.
			if cbcollectAlways || filesContainsAny([]string{results[i].logFile}, "server logs for details", "Timed out after 1m0s waiting for a bucket to become available") {
				if err := collectLogs(p.shortName(), clusterID, "cbcollect_"+p.fileName()+".zip"); err != nil {
					nonFatal.add(fmt.Errorf("cbcollect for %s: %w", p, err))
				}
			}
		})
	}

	wg.Wait()

	mergeJUnitXML(results, "integration.xml")

	logFiles := make([]string, 0, len(results))
	for _, r := range results {
		if r.logFile != "" {
			logFiles = append(logFiles, r.logFile)
		}
	}

	anyFailed := false
	for _, r := range results {
		if r.failed {
			anyFailed = true
		}
	}

	if anyFailed {
		logger.Warn("=== FAILED TESTS ===")
		for _, r := range results {
			if r.failed {
				logger.Warnf("Package: %s", r.path)
				for _, t := range r.failedTests {
					logger.Warnf("  --- FAIL: %s", t)
				}
			}
		}
	}

	logger.Info("=== PACKAGE DURATIONS ===")
	for _, r := range results {
		if r.duration > 0 {
			logger.Infof("  %-50s %s", r.path, r.duration.Round(time.Second))
		}
	}

	return anyFailed, logFiles
}

// runPackageIntegrationTests runs CBS integration tests for a single package.
// baseLogFileName is the log file name prefix (e.g. "verbose_int"); the package
// file name is appended to form the actual log and XML file paths.
// serverURL is the per-invocation Couchbase Server connection string.
// dockerName is the Docker container name for the KV node (empty string if not applicable).
// If ctx is cancelled, returns a non-failed result.
func runPackageIntegrationTests(ctx context.Context, edition Edition, flags []string, p pkg, baseLogFileName string, serverURL string, dockerName string) result {
	name := p.fileName()
	logFile := fmt.Sprintf("%s_%s.out", baseLogFileName, name)
	xmlFile := fmt.Sprintf("integration_%s.xml", name)
	args := goTestArgs{
		junitFile:        xmlFile,
		junitProjectName: "integration-" + p.shortName(),
		coverProfile:     "coverage_int_" + name + ".out",
		label:            p.shortName(),
		edition:          edition,
		packages:         []pkg{p},
		goTestFlags:      flags,
	}
	envExtras := []string{"SG_TEST_COUCHBASE_SERVER_URL=" + serverURL}
	if dockerName != "" {
		envExtras = append(envExtras, "SG_TEST_COUCHBASE_SERVER_DOCKER_NAME="+dockerName)
	}
	logger.Debugf("[%s] log=%s xml=%s cov=%s", p.shortName(), logFile, xmlFile, args.coverProfile)
	start := time.Now()
	failedTests, err := runGotestsum(ctx, args, logFile, envExtras...)
	duration := time.Since(start)
	if err != nil {
		if ctx.Err() != nil {
			return result{path: p, logFile: logFile, xmlFile: xmlFile, duration: duration}
		}
		logger.Errorf("[%s] go test failed: %v", p.shortName(), err)
		return result{path: p, logFile: logFile, xmlFile: xmlFile, failedTests: failedTests, duration: duration, failed: true}
	}
	return result{path: p, logFile: logFile, xmlFile: xmlFile, duration: duration}
}

// pkg is a fully-qualified SG package path, e.g. "github.com/couchbase/sync_gateway/rest".
type pkg string

// shortName returns the path relative to the module root using "/" as separator.
// Used for display: log labels, JUnit project names.
func (p pkg) shortName() string {
	short := strings.TrimPrefix(string(p), sgModule+"/")
	if short == "" || short == allPackages {
		return "all"
	}
	return short
}

// goTestPath returns the path to pass to go test: the module root is replaced with
// "./" so that the invocation is relative to the working directory.
func (p pkg) goTestPath() string {
	return strings.Replace(string(p), sgModule, "./", 1)
}

// fileName returns a filesystem-safe name, replacing "/" with "_".
// Used for log, coverage, and XML file names.
func (p pkg) fileName() string {
	short := strings.TrimPrefix(string(p), sgModule+"/")
	short = strings.ReplaceAll(short, "/", "_")
	if short == "" || short == allPackages {
		return "all"
	}
	return short
}

// mustRunRetry runs a command with retries and exponential backoff, and fatals if every attempt fails.
// It is for commands that hit the network.
func mustRunRetry(name string, args ...string) {
	const attempts = 4
	delay := 5 * time.Second
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err = runCommand(name, args...); err == nil {
			return
		}
		if attempt < attempts {
			logger.Warnf("attempt %d/%d failed: %v, retrying in %s", attempt, attempts, err, delay)
			time.Sleep(delay)
			delay *= 2
		}
	}
	logger.Fatalf("command failed after %d attempts: %v", attempts, err)
}

// runCommand runs a command, forwarding output to stdout/stderr.
func runCommand(name string, args ...string) error {
	return runLabeledCommand("", name, args...)
}

// runLabeledCommand runs a command. With a non-empty prefix, output is logged line by line
// with that prefix so concurrent package runs can be told apart.
func runLabeledCommand(prefix, name string, args ...string) error {
	cmd := exec.Command(name, args...) //nolint:gosec
	cmd.Env = mergeEnv()
	printCommand(prefix, cmd)
	if prefix == "" {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	}
	w := &labelWriter{prefix: prefix}
	defer w.flush()
	cmd.Stdout = w
	cmd.Stderr = w
	return cmd.Run()
}

// mustOutput runs a command and returns its combined stdout. Fatals on error.
func mustOutput(name string, args ...string) string {
	out, err := tryOutput(name, args...)
	if err != nil {
		logger.Fatalf("%v", err)
	}
	return out
}

// tryOutput runs a command and returns its combined stdout, or an error.
func tryOutput(name string, args ...string) (string, error) {
	return tryLabeledOutput("", name, args...)
}

// tryLabeledOutput is tryOutput with stderr logged under prefix when prefix is non-empty.
func tryLabeledOutput(prefix, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...) //nolint:gosec
	cmd.Env = mergeEnv()
	printCommand(prefix, cmd)
	if prefix == "" {
		cmd.Stderr = os.Stderr
	} else {
		w := &labelWriter{prefix: prefix}
		defer w.flush()
		cmd.Stderr = w
	}
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("command %q: %w", name, err)
	}
	return string(out), nil
}

// buildPackageList splits a comma-separated TARGET_PACKAGE value and prefixes each entry
// with the module path. Entries containing "..." are expanded via "go list".
func buildPackageList(targetPackage string) []pkg {
	parts := strings.Split(targetPackage, ",")
	pkgs := make([]pkg, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		fullPkg := sgModule + "/" + p
		if strings.Contains(p, allPackages) {
			expanded := mustOutput("go", "list", fullPkg)
			for line := range strings.SplitSeq(strings.TrimSpace(expanded), "\n") {
				if line = strings.TrimSpace(line); line != "" {
					pkgs = append(pkgs, pkg(line))
				}
			}
		} else {
			pkgs = append(pkgs, pkg(fullPkg))
		}
	}
	return pkgs
}

// filesContainsAny returns true if any of the given substrings appear in any of the files.
// Uses rg (ripgrep) if available on PATH, falling back to grep.
func filesContainsAny(filenames []string, substrings ...string) bool {
	if len(filenames) == 0 || len(substrings) == 0 {
		return false
	}
	tool, _ := exec.LookPath("rg")
	if tool == "" {
		tool = "grep"
	}
	args := make([]string, 0, 2+2*len(substrings)+len(filenames))
	args = append(args, "-q", "-F")
	for _, s := range substrings {
		args = append(args, "-e", s)
	}
	args = append(args, filenames...)
	cmd := exec.Command(tool, args...) //nolint:gosec
	return cmd.Run() == nil
}
