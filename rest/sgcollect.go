package rest

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

type sgCollectConfig struct {
	RedactLevel     string `json:"redact_level,omitempty"`
	RedactSalt      string `json:"redact_salt,omitempty"`
	OutputDirectory string `json:"output_dir,omitempty"`
	Upload          bool   `json:"upload,omitempty"`
	UploadHost      string `json:"upload_host,omitempty"`
	Customer        string `json:"customer,omitempty"`
	Ticket          string `json:"ticket,omitempty"`
}

// Args validates and returns a set of arguments to pass to sgcollect_info from a config.
func (c *sgCollectConfig) Args() (args []string, err error) {
	if c.OutputDirectory != "" {
		if fileInfo, err := os.Stat(c.OutputDirectory); err != nil {
			return nil, err
		} else if !fileInfo.IsDir() {
			return nil, errors.New("not a directory")
		}

		args = append(args, "-r", c.OutputDirectory)
	}

	if c.Upload {
		if c.UploadHost == "" {
			return nil, base.HTTPErrorf(http.StatusBadRequest, "Upload host must be set if upload is true.")
		}

		args = append(args, "--upload-host", c.UploadHost)
	}

	if c.RedactLevel != "" {
		args = append(args, "--log-redaction-level", c.RedactLevel)
	}

	if c.RedactSalt != "" {
		args = append(args, "--log-redaction-salt", c.RedactSalt)
	}

	if c.Customer != "" {
		args = append(args, "--customer", c.Customer)
	}

	if c.Ticket != "" {
		args = append(args, "--ticket", c.Ticket)
	}

	return args, nil
}

var sgcollectInstance sgCollectInstance

type sgCollectInstance struct {
	running bool
	lock    sync.Mutex
}

// SetRunning will mark the sgCollectInstance as running, if b is true, and is not already running.
func (i *sgCollectInstance) SetRunning(b bool) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	if i.running && b {
		return errors.New("sgcollect_info already running")
	}
	i.running = b
	return nil
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
