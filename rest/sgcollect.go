package rest

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"

	"github.com/couchbase/sync_gateway/base"
)

var (
	defualtSGUploadHost = "https://s3.amazonaws.com/cb-customers"

	// ErrSGCollectInfoAlreadyRunning is returned if sgcollect_info is already running.
	ErrSGCollectInfoAlreadyRunning = errors.New("already running")
	// ErrSGCollectInfoNotRunning is returned if sgcollect_info is not running.
	ErrSGCollectInfoNotRunning = errors.New("not running")

	sgcollectInstance = sgCollect{status: base.Uint32Ptr(sgStopped)}

	sgPath, sgCollectPath, _ = sgCollectPaths()
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

	if err := cmd.Start(); err != nil {
		return err
	}

	atomic.StoreUint32(sg.status, sgRunning)
	base.Infof(base.KeyAll, "sgcollect_info started with args: %v", base.UD(args))

	go func() {
		if err := cmd.Wait(); err != nil {
			base.Warnf(base.KeyAll, "sgcollect_info failed: %v", err)
		}
		atomic.StoreUint32(sg.status, sgStopped)
		base.Infof(base.KeyAll, "sgcollect_info finished")
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
func sgCollectPaths() (sgPath, sgCollectPath string, err error) {
	sgPath, err = os.Executable()
	if err != nil {
		return "", "", err
	}

	sgPath, err = filepath.Abs(sgPath)
	if err != nil {
		return "", "", err
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
		return "", "", err
	}

	return sgPath, sgCollectPath, nil
}
