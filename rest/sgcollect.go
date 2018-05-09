package rest

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"

	"github.com/couchbase/sync_gateway/base"
)

var (
	// ErrSGCollectInfoAlreadyRunning is returned if sgcollect_info is already running.
	ErrSGCollectInfoAlreadyRunning = errors.New("already running")
	// ErrSGCollectInfoNotRunning is returned if sgcollect_info is not running.
	ErrSGCollectInfoNotRunning = errors.New("not running")

	defualtSGUploadHost   = "https://s3.amazonaws.com/cb-customers"
	sgPath, sgCollectPath = sgCollectPaths()

	sgcollectInstance = sgCollect{status: base.Uint32Ptr(sgStopped)}
)

const (
	sgStopped uint32 = iota
	sgRunning
)

type sgCollect struct {
	cancel context.CancelFunc
	status *uint32
}

// Start will attempt to start sgcollect_info, if another is not already running.
func (sg *sgCollect) Start(filename string, args ...string) error {
	if atomic.LoadUint32(sg.status) == sgRunning {
		return ErrSGCollectInfoAlreadyRunning
	}

	args = append(args, "--sync-gateway-executable", sgPath, filename)

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

func (c *sgCollectOptions) setDefaults() {
	if c.UploadHost == "" {
		c.UploadHost = defualtSGUploadHost
	}
}

// Validate ensures the options are OK to use in sgcollect_info.
func (c *sgCollectOptions) Validate() error {
	c.setDefaults()

	if c.OutputDirectory != "" {
		if fileInfo, err := os.Stat(c.OutputDirectory); err != nil {
			return err
		} else if !fileInfo.IsDir() {
			return errors.New("not a directory")
		}
	}

	if c.Upload && c.Customer == "" {
		return errors.New("customer must be set if uploading")
	}

	return nil
}

// Args returns a set of arguments to pass to sgcollect_info.
func (c *sgCollectOptions) Args() []string {
	var args = make([]string, 0)

	if c.OutputDirectory != "" {
		args = append(args, "-r", c.OutputDirectory)
	}

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

// sgCollectPaths returns the absolute paths to Sync Gateway and to sgcollect_info.
func sgCollectPaths() (sgPath, sgCollectPath string) {
	sgPath, err := os.Executable()
	if err != nil {
		base.Warnf(base.KeyAll, "Unable to get path to SG executable. sgcollect_info may not contain all data nessesary for support.")
	}

	sgPath, err = filepath.Abs(sgPath)
	if err != nil {
		base.Warnf(base.KeyAll, "Unable to get absolute path to SG executable. sgcollect_info may not contain all data nessesary for support.")
	}

	// TODO: Validate this works on Windows
	sgCollectPath = filepath.Join("tools", "sgcollect_info")
	if runtime.GOOS == "windows" {
		sgCollectPath += ".exe"
	}

	sgCollectPath = filepath.Join(filepath.Dir(sgPath), sgCollectPath)

	// Make sure sgcollect_info exists
	_, err = os.Stat(sgCollectPath)
	if err != nil {
		base.Warnf(base.KeyAll, "Unable to find sgcollect_info executable")
	}

	return sgPath, sgCollectPath
}
