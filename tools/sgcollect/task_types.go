package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"
)

// SGCollectTask is the base implementation of a task.
type SGCollectTask interface {
	Name() string
	Header() string
	OutputFile() string
	Run(ctx context.Context, opts *SGCollectOptions, out io.Writer) error
}

// SGCollectTaskEx wraps a SGCollectTask with information about how it should be run.
type SGCollectTaskEx interface {
	SGCollectTask
	ShouldRun(platform string) bool
	RequiresRoot() bool
	NumSamples() int
	Interval() time.Duration
	Timeout() time.Duration
}

// TaskEx wraps the given SGCollectTask in a SGCollectTaskEx if necessary.
func TaskEx(t SGCollectTask) SGCollectTaskEx {
	if ex, ok := t.(SGCollectTaskEx); ok {
		return ex
	}
	return ex{
		SGCollectTask: t,
	}
}

type ex struct {
	SGCollectTask
	platforms []string
	root      bool
	samples   int
	interval  time.Duration
	timeout   time.Duration
}

func (e ex) ShouldRun(platform string) bool {
	if len(e.platforms) == 0 {
		return true
	}
	for _, plat := range e.platforms {
		if plat == platform {
			return true
		}
	}
	return false
}

func (e ex) RequiresRoot() bool {
	return e.root
}

func (e ex) NumSamples() int {
	return e.samples
}

func (e ex) Interval() time.Duration {
	return e.interval
}

func (e ex) Timeout() time.Duration {
	return e.timeout
}

func Sample(t SGCollectTask, samples int, interval time.Duration) SGCollectTaskEx {
	if ex, ok := t.(ex); ok {
		ex.samples = samples
		ex.interval = interval
		return ex
	}
	return ex{
		SGCollectTask: t,
		samples:       samples,
		interval:      interval,
	}
}

func Timeout(t SGCollectTask, timeout time.Duration) SGCollectTaskEx {
	if ex, ok := t.(ex); ok {
		ex.timeout = timeout
		return ex
	}
	return ex{
		SGCollectTask: t,
		timeout:       timeout,
	}
}

func Privileged(t SGCollectTask) SGCollectTaskEx {
	if ex, ok := t.(ex); ok {
		ex.root = true
		return ex
	}
	return ex{
		SGCollectTask: t,
		root:          true,
	}
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

func (c *URLTask) Run(ctx context.Context, opts *SGCollectOptions, out io.Writer) error {
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
	_, err = Copier(opts)(out, res.Body)
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

func (f *FileTask) Run(ctx context.Context, opts *SGCollectOptions, out io.Writer) error {
	fd, err := os.Open(f.inputFile)
	if err != nil {
		return fmt.Errorf("failed to open %q: %w", f.inputFile, err)
	}
	defer fd.Close()

	_, err = Copier(opts)(out, fd)
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

func (f *GZipFileTask) Run(ctx context.Context, opts *SGCollectOptions, out io.Writer) error {
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

	_, err = Copier(opts)(out, unzipper)
	if err != nil {
		return fmt.Errorf("failed to copy contents of %q: %w", f.inputFile, err)
	}
	return nil
}

type OSCommandTask struct {
	name       string
	command    string
	outputFile string
}

func (o *OSCommandTask) Name() string {
	return o.name
}

func (o *OSCommandTask) Header() string {
	return o.command
}

func (o *OSCommandTask) OutputFile() string {
	return o.outputFile
}

func (o *OSCommandTask) Run(ctx context.Context, opts *SGCollectOptions, out io.Writer) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	cmd := exec.Command("sh", "-c", o.command)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}
	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start: %w", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		defer wg.Done()
		_, err := Copier(opts)(out, stdout)
		if err != nil {
			log.Printf("WARN %s [%s]: stdout copier error %v", o.name, o.Header(), err)
		}
	}()
	go func() {
		defer wg.Done()
		_, err := Copier(opts)(out, stderr)
		if err != nil {
			log.Printf("WARN %s [%s]: stderr copier error %v", o.name, o.Header(), err)
		}
	}()
	go func() {
		defer wg.Done()
		err = cmd.Wait()
		cancel() // to release the below goroutine
	}()
	go func() {
		defer wg.Done()
		<-ctx.Done()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}()
	wg.Wait()
	return err
}

type osTask struct {
	OSCommandTask
	platforms []string
}

func (o *osTask) ShouldRun(platform string) bool {
	for _, p := range o.platforms {
		if p == platform {
			return true
		}
	}
	return false
}

// OSTask is a helper to return a task for the given platform, which can be one of
// "unix", "linux, "windows", "solaris", or "" for all platforms.
func OSTask(platform string, name, cmd string) SGCollectTask {
	switch platform {
	case "":
		return &OSCommandTask{name, cmd, ""}
	case "linux", "windows":
		return &osTask{OSCommandTask{name, cmd, ""}, []string{platform}}
	case "unix":
		return &osTask{OSCommandTask{name, cmd, ""}, []string{"linux", "darwin", "freebsd", "netbsd", "openbsd"}}
	case "solaris":
		return &osTask{OSCommandTask{name, cmd, ""}, []string{"illumos", "solaris"}}
	default:
		panic(fmt.Sprintf("unknown platform %s", platform))
	}
}

type SGCollectOptionsTask struct{}

func (s SGCollectOptionsTask) Name() string {
	return "sgcollect_info options"
}

func (s SGCollectOptionsTask) Header() string {
	return ""
}

func (s SGCollectOptionsTask) OutputFile() string {
	return "sgcollect_info_options.log"
}

func (s SGCollectOptionsTask) Run(ctx context.Context, opts *SGCollectOptions, out io.Writer) error {
	_, err := fmt.Fprintf(out, "%#v\n", opts)
	return err
}
