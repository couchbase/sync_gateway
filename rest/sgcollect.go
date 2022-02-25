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
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
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

	sgPath, sgCollectPath, sgCollectPathErr = sgCollectPaths()
	sgcollectInstance                       = sgCollect{
		status:        base.Uint32Ptr(sgStopped),
		sgPath:        sgPath,
		sgCollectPath: sgCollectPath,
		pathError:     sgCollectPathErr,
	}
)

const (
	sgStopped uint32 = iota
	sgRunning

	defaultSGUploadHost = "https://uploads.couchbase.com"
)

type sgCollect struct {
	cancel        context.CancelFunc
	status        *uint32
	sgPath        string
	sgCollectPath string
	pathError     error
	context       context.Context
}

// Start will attempt to start sgcollect_info, if another is not already running.
func (sg *sgCollect) Start(logFilePath string, ctxSerialNumber uint64, zipFilename string, params sgCollectOptions) error {
	if atomic.LoadUint32(sg.status) == sgRunning {
		return ErrSGCollectInfoAlreadyRunning
	}

	// Return error if there is any failure while obtaining sgCollectPaths.
	if sg.pathError != nil {
		return sg.pathError
	}

	if params.OutputDirectory == "" {
		// If no output directory specified, default to the configured LogFilePath
		if logFilePath != "" {
			params.OutputDirectory = logFilePath
			base.DebugfCtx(sg.context, base.KeyAdmin, "sgcollect_info: no output directory specified, using LogFilePath: %v", params.OutputDirectory)
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

	args := params.Args()
	args = append(args, "--sync-gateway-executable", sgPath)
	args = append(args, zipPath)

	ctx := context.WithValue(context.Background(), base.LogContextKey{}, base.LogContext{CorrelationID: fmt.Sprintf("SGCollect-%03d", ctxSerialNumber)})

	sg.context, sg.cancel = context.WithCancel(ctx)
	cmd := exec.CommandContext(sg.context, sgCollectPath, args...)

	// Send command stderr/stdout to pipes
	stderrPipeReader, stderrPipeWriter := io.Pipe()
	cmd.Stderr = stderrPipeWriter
	stdoutPipeReader, stdoutpipeWriter := io.Pipe()
	cmd.Stdout = stdoutpipeWriter

	if err := cmd.Start(); err != nil {
		return err
	}

	atomic.StoreUint32(sg.status, sgRunning)
	startTime := time.Now()
	base.InfofCtx(sg.context, base.KeyAdmin, "sgcollect_info started with args: %v", base.UD(args))

	// Stream sgcollect_info stderr to warn logs
	go func() {
		scanner := bufio.NewScanner(stderrPipeReader)
		for scanner.Scan() {
			base.InfofCtx(sg.context, base.KeyAll, "sgcollect_info: %v", scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			base.ErrorfCtx(sg.context, "sgcollect_info: unexpected error: %v", err)
		}
	}()

	// Stream sgcollect_info stdout to debug logs
	go func() {
		scanner := bufio.NewScanner(stdoutPipeReader)
		for scanner.Scan() {
			base.InfofCtx(sg.context, base.KeyAll, "sgcollect_info: %v", scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			base.ErrorfCtx(sg.context, "sgcollect_info: unexpected error: %v", err)
		}
	}()

	go func() {
		// Blocks until command finishes
		err := cmd.Wait()

		atomic.StoreUint32(sg.status, sgStopped)
		duration := time.Since(startTime)

		if err != nil {
			if err.Error() == "signal: killed" {
				base.InfofCtx(sg.context, base.KeyAdmin, "sgcollect_info cancelled after %v", duration)
				return
			}

			base.ErrorfCtx(sg.context, "sgcollect_info failed after %v with reason: %v. Check warning level logs for more information.", duration, err)
			return
		}

		base.InfofCtx(sg.context, base.KeyAdmin, "sgcollect_info finished successfully after %v", duration)
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

type sgCollectOptions struct {
	RedactLevel     string `json:"redact_level,omitempty"`
	RedactSalt      string `json:"redact_salt,omitempty"`
	OutputDirectory string `json:"output_dir,omitempty"`
	Upload          bool   `json:"upload,omitempty"`
	UploadHost      string `json:"upload_host,omitempty"`
	UploadProxy     string `json:"upload_proxy,omitempty"`
	Customer        string `json:"customer,omitempty"`
	Ticket          string `json:"ticket,omitempty"`

	// Unexported - Don't allow these to be set via the JSON body.
	// We'll set them from the request's basic auth.
	syncGatewayUsername string
	syncGatewayPassword string
}

// validateOutputDirectory will check that the given path exists, and is a directory.
func validateOutputDirectory(dir string) error {
	// Clean the given path first, mainly for cross-platform compatability.
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

// Validate ensures the options are OK to use in sgcollect_info.
func (c *sgCollectOptions) Validate() error {

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
			c.UploadHost = defaultSGUploadHost
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
func (c *sgCollectOptions) Args() []string {
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

	if c.syncGatewayUsername != "" {
		args = append(args, "--sync-gateway-username", c.syncGatewayUsername)
	}

	if c.syncGatewayPassword != "" {
		args = append(args, "--sync-gateway-password", c.syncGatewayPassword)
	}

	return args
}

// sgCollectPaths attempts to return the absolute paths to Sync Gateway and to sgcollect_info binaries.
func sgCollectPaths() (sgBinary, sgCollectBinary string, err error) {
	sgBinary, err = os.Executable()
	if err != nil {
		return "", "", err
	}

	sgBinary, err = filepath.Abs(sgBinary)
	if err != nil {
		return "", "", err
	}

	logCtx := context.Background()
	hasBinDir := true
	sgCollectPath := filepath.Join("tools", "sgcollect_info")

	if runtime.GOOS == "windows" {
		sgCollectPath += ".exe"
		// Windows has no bin directory for the SG executable.
		hasBinDir = false
	}

	for {
		if hasBinDir {
			sgCollectBinary = filepath.Join(filepath.Dir(filepath.Dir(sgBinary)), sgCollectPath)
		} else {
			sgCollectBinary = filepath.Join(filepath.Dir(sgBinary), sgCollectPath)
		}

		// Check sgcollect_info exists at the path we guessed.
		base.DebugfCtx(logCtx, base.KeyAdmin, "Checking sgcollect_info binary exists at: %v", sgCollectBinary)
		_, err = os.Stat(sgCollectBinary)
		if err != nil {

			// First attempt may fail if there's no bin directory, so we'll try once more without.
			if hasBinDir {
				hasBinDir = false
				continue
			}

			return "", "", err
		}

		return sgBinary, sgCollectBinary, nil
	}
}

// sgcollectFilename returns a Windows-safe filename for sgcollect_info zip files.
func sgcollectFilename() string {

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
