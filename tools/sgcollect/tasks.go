package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type SGCollectTask interface {
	Name() string
	Header() string
	OutputFile() string
	Run(opts *SGCollectOptions, out io.Writer) error
}

type PlatformTask interface {
	ShouldRun(platform string) bool
}

type PrivilegedTask interface {
	RequiresRoot()
}

type SampledTask interface {
	NumSamples() int
	Interval() time.Duration
}

type URLTask struct {
	name       string
	url        string
	outputFile string
}

func (c *URLTask) Name() string {
	return c.name
}

func (c *URLTask) Header() string {
	return c.url
}

func (c *URLTask) OutputFile() string {
	return c.outputFile
}

func (c *URLTask) Run(opts *SGCollectOptions, out io.Writer) error {
	req, err := http.NewRequest(http.MethodGet, c.url, nil)
	if err != nil {
		return fmt.Errorf("failed to build HTTP request: %w", err)
	}
	req.SetBasicAuth(opts.SyncGatewayUsername, string(opts.SyncGatewayPassword))

	res, err := getHTTPClient(opts).Do(req)
	if err != nil {
		return fmt.Errorf("failed to request: %w", err)
	}
	defer res.Body.Close()
	_, err = io.Copy(out, res.Body)
	if err != nil {
		return fmt.Errorf("failed to load response: %w", err)
	}
	return nil
}

type FileTask struct {
	name                  string
	inputFile, outputFile string
}

func (f *FileTask) Name() string {
	return f.name
}

func (f *FileTask) Header() string {
	return f.inputFile
}

func (f *FileTask) OutputFile() string {
	return f.outputFile
}

func (f *FileTask) Run(_ *SGCollectOptions, out io.Writer) error {
	fd, err := os.Open(f.inputFile)
	if err != nil {
		return fmt.Errorf("failed to open %q: %w", f.inputFile, err)
	}
	defer fd.Close()

	_, err = io.Copy(out, fd)
	if err != nil {
		return fmt.Errorf("failed to copy contents of %q: %w", f.inputFile, err)
	}
	return nil
}

type GZipFileTask struct {
	name                  string
	inputFile, outputFile string
}

func (f *GZipFileTask) Name() string {
	return f.name
}

func (f *GZipFileTask) Header() string {
	return f.inputFile
}

func (f *GZipFileTask) OutputFile() string {
	return f.outputFile
}

func (f *GZipFileTask) Run(_ *SGCollectOptions, out io.Writer) error {
	fd, err := os.Open(f.inputFile)
	if err != nil {
		return fmt.Errorf("failed to open %q: %w", f.inputFile, err)
	}
	defer fd.Close()

	unzipper, err := gzip.NewReader(fd)
	if err != nil {
		return fmt.Errorf("failed to decompress %q: %w", f.inputFile, err)
	}
	defer unzipper.Close()

	_, err = io.Copy(out, unzipper)
	if err != nil {
		return fmt.Errorf("failed to copy contents of %q: %w", f.inputFile, err)
	}
	return nil
}
