/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/pkg/errors"
)

var (
	// ErrSGCollectInfoAlreadyRunning is returned if sgcollect_info is already running.
	ErrSGCollectInfoAlreadyRunning = errors.New("already running")
	// ErrSGCollectInfoNotRunning is returned if sgcollect_info is not running.
	ErrSGCollectInfoNotRunning = errors.New("not running")

	validateTicketPattern = regexp.MustCompile(`\d{1,7}`)

	sgcollectTokenEnvVar = "SGCOLLECT_TOKEN" // Environment variable to set the token for sgcollect_info
)

const (
	sgStopped uint32 = iota
	sgRunning

	DefaultSGCollectUploadHost = "https://uploads.couchbase.com"

	sgcollectTokenTimeout = 12 * time.Hour
)

// sgCollectOutputStream handles stderr/stdout from a running sgcollect process.
type sgCollectOutputStream struct {
	stdoutPipeWriter io.WriteCloser // Pipe writer for stdout
	stderrPipeWriter io.WriteCloser // Pipe writer for stderr
	stderrPipeReader io.Reader      // Pipe reader for stderr
	stdoutPipeReader io.Reader      // Pipe reader for stdout
	stdoutDoneChan   chan struct{}  // Channel to signal stdout processing completion
	stderrDoneChan   chan struct{}  // Channel to signal stderr processing completion
}

// sgCollect manages the state of a running sgcollect_info process.
type sgCollect struct {
	cancel           context.CancelFunc // Function to cancel a running sgcollect_info process, set when status == sgRunning
	status           *uint32
	sgPath           string    // Path to the Sync Gateway executable
	SGCollectPath    []string  // Path to the sgcollect_info executable
	SGCollectPathErr error     // Error if sgcollect_info path could not be determined
	Token            string    // Token for sgcollect_info, if required
	tokenAge         time.Time // Time when the token was created
	Stdout           io.Writer // test seam, is nil in production
	Stderr           io.Writer // test seam, is nil in production
}

// Start will attempt to start sgcollect_info, if another is not already running.
func (sg *sgCollect) Start(ctx context.Context, logFilePath string, zipFilename string, params SGCollectOptions) error {
	if atomic.LoadUint32(sg.status) == sgRunning {
		return ErrSGCollectInfoAlreadyRunning
	}

	// Return error if there is any failure while obtaining sgCollectPaths.
	if sg.SGCollectPathErr != nil {
		return sg.SGCollectPathErr
	}

	if params.OutputDirectory == "" {
		// If no output directory specified, default to the configured LogFilePath
		if logFilePath != "" {
			params.OutputDirectory = logFilePath
			base.DebugfCtx(ctx, base.KeyAdmin, "sgcollect_info: no output directory specified, using LogFilePath: %v", params.OutputDirectory)
		} else {
			// If LogFilePath is not set, and DefaultLogFilePath is not set via a service script, error out.
			return errors.New("no output directory or LogFilePath specified")
		}

		// Validate the path, just in case were not getting it correctly.
		if err := validateOutputDirectory(params.OutputDirectory); err != nil {
			return err
		}
	}

	zipPath := filepath.Join(params.OutputDirectory, zipFilename)

	cmdline := slices.Clone(sg.SGCollectPath)
	cmdline = append(cmdline, params.Args()...)
	cmdline = append(cmdline, "--sync-gateway-executable", sg.sgPath)
	cmdline = append(cmdline, zipPath)

	ctx, sg.cancel = context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, cmdline[0], cmdline[1:]...)

	err := sg.createNewToken()
	if err != nil {
		return fmt.Errorf("failed to get sgcollect_info token: %w", err)
	}
	cmd.Env = append(os.Environ(),
		sgcollectTokenEnvVar+"="+sg.Token,
	)
	outStream := newSGCollectOutputStream(ctx, sg.Stdout, sg.Stderr)
	cmd.Stdout = outStream.stdoutPipeWriter
	cmd.Stderr = outStream.stderrPipeWriter

	if err := cmd.Start(); err != nil {
		outStream.Close(ctx)
		return err
	}

	atomic.StoreUint32(sg.status, sgRunning)
	startTime := time.Now()
	base.InfofCtx(ctx, base.KeyAdmin, "sgcollect_info started with output zip: %v", base.UD(zipPath))

	go func() {
		defer sg.clearToken()
		// Blocks until command finishes
		err := cmd.Wait()
		outStream.Close(ctx)

		atomic.StoreUint32(sg.status, sgStopped)
		duration := time.Since(startTime)

		if err != nil {
			if err.Error() == "signal: killed" {
				base.InfofCtx(ctx, base.KeyAdmin, "sgcollect_info cancelled after %v", duration)
				return
			}

			base.ErrorfCtx(ctx, "sgcollect_info failed after %v with reason: %v. Check warning level logs for more information.", duration, err)
			return
		}

		base.InfofCtx(ctx, base.KeyAdmin, "sgcollect_info finished successfully after %v", duration)
	}()

	return nil
}

// Stop will stop sgcollect_info, if running.
func (sg *sgCollect) Stop() error {
	if atomic.LoadUint32(sg.status) == sgStopped {
		return ErrSGCollectInfoNotRunning
	}

	sg.cancel()
	atomic.StoreUint32(sg.status, sgStopped)

	return nil
}

// IsRunning returns true if sgcollect_info is running
func (sg *sgCollect) IsRunning() bool {
	return atomic.LoadUint32(sg.status) == sgRunning
}

// createNewToken creates a token specific to running sgcollect.
func (sg *sgCollect) createNewToken() error {
	token, err := base.GenerateRandomSecret()
	if err != nil {
		sg.clearToken()
	}
	sg.Token = token
	sg.tokenAge = time.Now()
	return err
}

// clearToken clears the token.
func (sg *sgCollect) clearToken() {
	sg.Token = ""
	sg.tokenAge = time.Time{}
}

// getToken returns an sgcollect specific token from headers, if found. If not found, returns an empty string.
func (sg *sgCollect) getToken(headers http.Header) string {
	auth := headers.Get("Authorization")
	if auth == "" {
		return ""
	}
	authPrefix := "SGCollect "
	if !strings.HasPrefix(auth, authPrefix) {
		return ""
	}
	return auth[len(authPrefix):]
}

// hasValidToken checks if the provided headers contain a valid token for sgcollect_info.
func (sg *sgCollect) hasValidToken(ctx context.Context, token string) bool {
	if time.Since(sg.tokenAge) > sgcollectTokenTimeout {
		base.DebugfCtx(ctx, base.KeyAdmin, "sgcollect_info token has expired after %.2f secs", time.Since(sg.tokenAge).Seconds())
		return false
	}
	return token == sg.Token
}

type SGCollectOptions struct {
	RedactLevel     string `json:"redact_level,omitempty"`
	RedactSalt      string `json:"redact_salt,omitempty"`
	OutputDirectory string `json:"output_dir,omitempty"`
	Upload          bool   `json:"upload,omitempty"`
	UploadHost      string `json:"upload_host,omitempty"`
	UploadProxy     string `json:"upload_proxy,omitempty"`
	Customer        string `json:"customer,omitempty"`
	Ticket          string `json:"ticket,omitempty"`
	KeepZip         bool   `json:"keep_zip,omitempty"`

	adminURL string // URL to the Sync Gateway admin API.
}

// validateOutputDirectory will check that the given path exists, and is a directory.
func validateOutputDirectory(dir string) error {
	// Clean the given path first, mainly for cross-platform compatibility.
	dir = filepath.Clean(dir)

	// Validate given output directory exists, and is a directory.
	// This does not check for write permission, however sgcollect_info
	// will fail with an error giving that reason, if this is the case.
	if fileInfo, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return errors.Wrap(err, "no such file or directory")
		}
		return err
	} else if !fileInfo.IsDir() {
		return errors.New("not a directory")
	}

	return nil
}

// newSGCollectOutputStream creates an instance to monitor stdout and stderr. Stdout is logged at Debug and Stderr at Info. extraStdout and extraStderr are optional writers used for testing only.
func newSGCollectOutputStream(ctx context.Context, extraStdout io.Writer, extraStderr io.Writer) *sgCollectOutputStream {
	stderrPipeReader, stderrPipeWriter := io.Pipe()
	stdoutPipeReader, stdoutPipeWriter := io.Pipe()
	o := &sgCollectOutputStream{
		stdoutPipeWriter: stdoutPipeWriter,
		stderrPipeWriter: stderrPipeWriter,
		stderrPipeReader: stderrPipeReader,
		stdoutPipeReader: stdoutPipeReader,
		stdoutDoneChan:   make(chan struct{}),
		stderrDoneChan:   make(chan struct{}),
	}
	go func() {
		defer close(o.stderrDoneChan)
		scanner := bufio.NewScanner(stderrPipeReader)
		for scanner.Scan() {
			text := scanner.Text()
			base.InfofCtx(ctx, base.KeyAll, "sgcollect_info: %v", text)
			if extraStderr != nil {
				_, err := extraStderr.Write([]byte(text + "\n"))
				if err != nil {
					base.ErrorfCtx(ctx, "sgcollect_info: failed to write to stderr pipe: %v", err)
				}
			}
		}
		if err := scanner.Err(); err != nil {
			base.ErrorfCtx(ctx, "sgcollect_info: unexpected error: %v", err)
		}
	}()

	// Stream sgcollect_info stdout to debug logs
	go func() {
		defer close(o.stdoutDoneChan)
		scanner := bufio.NewScanner(stdoutPipeReader)
		for scanner.Scan() {
			text := scanner.Text()
			base.InfofCtx(ctx, base.KeyAll, "sgcollect_info: %v", text)
			if extraStdout != nil {
				_, err := extraStdout.Write([]byte(text + "\n"))
				if err != nil {
					base.ErrorfCtx(ctx, "sgcollect_info: failed to write to stdout pipe: %v", err)
				}
			}
		}
		if err := scanner.Err(); err != nil {
			base.ErrorfCtx(ctx, "sgcollect_info: unexpected error: %v", err)
		}
	}()
	return o
}

// Close the output streams, required to close goroutines when sgCollectOutputStream is created.
func (o *sgCollectOutputStream) Close(ctx context.Context) {
	err := o.stderrPipeWriter.Close()
	if err != nil {
		base.WarnfCtx(ctx, "sgcollect_info: failed to close stderr pipe writer: %v", err)
	}
	err = o.stdoutPipeWriter.Close()
	if err != nil {
		base.WarnfCtx(ctx, "sgcollect_info: failed to close stdout pipe writer: %v", err)
	}
	// Wait for the goroutines to finish processing the output streams, or exit after 5 seconds.
	select {
	case <-o.stdoutDoneChan:
	case <-time.After(5 * time.Second):
		base.WarnfCtx(ctx, "sgcollect_info: timed out waiting for stdout processing to finish")
	}
	select {
	case <-o.stderrDoneChan:
	case <-time.After(5 * time.Second):
		base.WarnfCtx(ctx, "sgcollect_info: timed out waiting for stderr processing to finish")
	}
}

// Validate ensures the options are OK to use in sgcollect_info.
func (c *SGCollectOptions) Validate() error {

	var errs *base.MultiError
	if c.OutputDirectory != "" {
		if err := validateOutputDirectory(c.OutputDirectory); err != nil {
			errs = errs.Append(err)
		}
	}

	if c.Ticket != "" {
		if !validateTicketPattern.MatchString(c.Ticket) {
			errs = errs.Append(errors.New("'ticket' must be 1 to 7 digits"))
		}
	}

	if c.Upload {
		// Customer number is required if uploading.
		if c.Customer == "" {
			errs = errs.Append(errors.New("'customer' must be set if upload is true"))
		}
		// Default uploading to support bucket if upload_host is not specified.
		if c.UploadHost == "" {
			c.UploadHost = DefaultSGCollectUploadHost
		}
	} else {
		// These fields suggest the user actually wanted to upload,
		// so we'll enforce "upload: true" if any of these are set.
		if c.UploadHost != "" {
			errs = errs.Append(errors.New("'upload' must be set to true if 'upload_host' is specified"))
		}
		if c.Customer != "" {
			errs = errs.Append(errors.New("'upload' must be set to true if 'customer' is specified"))
		}
		if c.Ticket != "" {
			errs = errs.Append(errors.New("'upload' must be set to true if 'ticket' is specified"))
		}
	}

	if c.RedactLevel != "" && c.RedactLevel != "none" && c.RedactLevel != "partial" {
		errs = errs.Append(errors.New("'redact_level' must be either 'none' or 'partial'"))
	}

	return errs.ErrorOrNil()
}

// Args returns a set of arguments to pass to sgcollect_info.
func (c *SGCollectOptions) Args() []string {
	var args = make([]string, 0)

	if c.Upload {
		args = append(args, "--upload-host", c.UploadHost)
	}

	if c.UploadProxy != "" {
		args = append(args, "--upload-proxy", c.UploadProxy)
	}

	if c.Customer != "" {
		args = append(args, "--customer", c.Customer)
	}

	if c.Ticket != "" {
		args = append(args, "--ticket", c.Ticket)
	}

	if c.RedactLevel != "" {
		args = append(args, "--log-redaction-level", c.RedactLevel)
	}

	if c.RedactSalt != "" {
		args = append(args, "--log-redaction-salt", c.RedactSalt)
	}

	if c.KeepZip {
		args = append(args, "--keep-zip")
	}
	if c.adminURL != "" {
		args = append(args, "--sync-gateway-url", c.adminURL)
	}
	return args
}

// sgCollectPaths attempts to return the absolute paths to Sync Gateway and to sgcollect_info binaries. Returns an error if either cannot be found.
//
// The sgcollect_info return value is allowed to be a list of strings for testing, where is it , or an error if not.
func sgCollectPaths(ctx context.Context) (sgBinary string, sgCollect []string, err error) {
	sgBinary, err = os.Executable()
	if err != nil {
		return "", nil, err
	}

	sgBinary, err = filepath.Abs(sgBinary)
	if err != nil {
		return "", nil, err
	}

	hasBinDir := true
	sgCollectPath := filepath.Join("tools", "sgcollect_info")

	if runtime.GOOS == "windows" {
		sgCollectPath += ".exe"
		// Windows has no bin directory for the SG executable.
		hasBinDir = false
	}

	for {
		var sgCollectBinary string
		if hasBinDir {
			sgCollectBinary = filepath.Join(filepath.Dir(filepath.Dir(sgBinary)), sgCollectPath)
		} else {
			sgCollectBinary = filepath.Join(filepath.Dir(sgBinary), sgCollectPath)
		}

		// Check sgcollect_info exists at the path we guessed.
		base.DebugfCtx(ctx, base.KeyAdmin, "Checking sgcollect_info binary exists at: %v", sgCollectBinary)
		_, err = os.Stat(sgCollectBinary)
		if err != nil {

			// First attempt may fail if there's no bin directory, so we'll try once more without.
			if hasBinDir {
				hasBinDir = false
				continue
			}

			return "", nil, err
		}

		return sgBinary, []string{sgCollectBinary}, nil
	}
}

// SGCollectFilename returns a Windows-safe filename for sgcollect_info zip files.
func SGCollectFilename() string {

	// get timestamp
	timestamp := time.Now().UTC().Format("2006-01-02t150405")

	// use a shortened product name
	name := "sg"

	// get primary IP address
	ip, err := base.FindPrimaryAddr()
	if err != nil {
		ip = net.IPv4zero
	}

	// E.g: sgcollectinfo-2018-05-10t133456-sg@203.0.113.123.zip
	filename := fmt.Sprintf("sgcollectinfo-%s-%s@%s.zip", timestamp, name, ip)

	// Strip illegal Windows filename characters
	filename = base.ReplaceAll(filename, "\\/:*?\"<>|", "")

	return filename
}

// newSGCollect creates a new sgCollect instance.
func newSGCollect(ctx context.Context) *sgCollect {
	sgCollectInstance := sgCollect{
		status: base.Ptr(sgStopped),
	}
	sgCollectInstance.sgPath, sgCollectInstance.SGCollectPath, sgCollectInstance.SGCollectPathErr = sgCollectPaths(ctx)
	return &sgCollectInstance
}
