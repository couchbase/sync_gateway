package rest

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

var (
	// ErrSGCollectInfoAlreadyRunning is returned if sgcollect_info is already running.
	ErrSGCollectInfoAlreadyRunning = errors.New("already running")
	// ErrSGCollectInfoNotRunning is returned if sgcollect_info is not running.
	ErrSGCollectInfoNotRunning = errors.New("not running")

	sgcollectInstance = sgCollect{status: base.Uint32Ptr(sgStopped)}
)

const (
	sgStopped uint32 = iota
	sgRunning

	defaultSGUploadHost = "https://s3.amazonaws.com/cb-customers"
)

type sgCollect struct {
	cancel context.CancelFunc
	status *uint32
}

// Start will attempt to start sgcollect_info, if another is not already running.
func (sg *sgCollect) Start(zipPath string, args ...string) error {
	if atomic.LoadUint32(sg.status) == sgRunning {
		return ErrSGCollectInfoAlreadyRunning
	}

	sgPath, sgCollectPath, err := sgCollectPaths()
	if err != nil {
		return err
	}

	args = append(args, "--sync-gateway-executable", sgPath, zipPath)

	ctx, cancelFunc := context.WithCancel(context.Background())
	sg.cancel = cancelFunc
	cmd := exec.CommandContext(ctx, sgCollectPath, args...)

	// Send command stderr/stdout to pipe
	pipeReader, pipeWriter := io.Pipe()
	cmd.Stderr = pipeWriter
	cmd.Stdout = pipeWriter

	if err := cmd.Start(); err != nil {
		return err
	}

	atomic.StoreUint32(sg.status, sgRunning)
	base.Infof(base.KeyAdmin, "sgcollect_info started with args: %v", base.UD(args))

	// Stream sgcollect_info output to the logs
	go func() {
		scanner := bufio.NewScanner(pipeReader)
		for scanner.Scan() {
			base.Debugf(base.KeyAdmin, "sgcollect_info: %v", scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			base.Warnf(base.KeyAdmin, "sgcollect_info: unexpected error: %v", err)
		}
	}()

	go func() {
		// Blocks until command finishes
		err := cmd.Wait()

		atomic.StoreUint32(sg.status, sgStopped)
		duration := cmd.ProcessState.UserTime()

		if err != nil {
			if err.Error() == "signal: killed" {
				base.Infof(base.KeyAdmin, "sgcollect_info cancelled after %v", duration)
				return
			}

			base.Warnf(base.KeyAdmin, "sgcollect_info failed after %v with reason: %v. Check debug logs with log key 'Admin' for more detail.", duration, err)
			return
		}

		base.Infof(base.KeyAdmin, "sgcollect_info finished successfully after %v", duration)
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
	Customer        string `json:"customer,omitempty"`
	Ticket          string `json:"ticket,omitempty"`
}

// Validate ensures the options are OK to use in sgcollect_info.
func (c *sgCollectOptions) Validate() error {
	// Validate given output directory exists, and is a directory.
	// This does not check for write permission, however sgcollect_info
	// will fail with an error giving that reason, if this is the case.
	if c.OutputDirectory != "" {
		if fileInfo, err := os.Stat(c.OutputDirectory); err != nil {
			return err
		} else if !fileInfo.IsDir() {
			return errors.New("not a directory")
		}
	}

	if c.Upload {
		// Customer number is required if uploading.
		if c.Customer == "" {
			return errors.New("customer must be set if upload is true")
		}
		// Default uploading to support bucket if upload_host is not specified.
		if c.UploadHost == "" {
			c.UploadHost = defaultSGUploadHost
		}
	} else if c.UploadHost != "" {
		return errors.New("upload must be set to true if an upload_host is specified")
	}

	return nil
}

// Args returns a set of arguments to pass to sgcollect_info.
func (c *sgCollectOptions) Args() []string {
	var args = make([]string, 0)

	if c.Upload {
		args = append(args, "--upload-host", c.UploadHost)
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
		base.Debugf(base.KeyAdmin, "Checking sgcollect_info binary exists at: %v", sgCollectBinary)
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
	if base.ProductName == "Couchbase SG Accel" {
		name = "sga"
	}

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
