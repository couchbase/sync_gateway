package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
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
	Run(ctx context.Context, opts *SGCollectOptions, out io.Writer) error
}

// SGCollectTaskEx adds metadata to a task, such as platforms it's applicable for.
// This should not be created directly, instead use one of the helpers (such as Privileged or Sample), which take care
// of setting the properties on the task if it is already a SGCollectTaskEx.
type SGCollectTaskEx struct {
	SGCollectTask
	platforms   []string
	root        bool
	samples     int
	interval    time.Duration
	timeout     time.Duration
	outputFile  string
	noHeader    bool
	mayFailTest bool
}

// TaskEx wraps the given SGCollectTask in a SGCollectTaskEx, or returns it if it is already a SGCollectTaskEx.
func TaskEx(t SGCollectTask) SGCollectTaskEx {
	if ex, ok := t.(SGCollectTaskEx); ok {
		return ex
	}
	return SGCollectTaskEx{
		SGCollectTask: t,
	}
}

func (e SGCollectTaskEx) ShouldRun(platform string) bool {
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

func (e SGCollectTaskEx) RequiresRoot() bool {
	return e.root
}

func (e SGCollectTaskEx) NumSamples() int {
	return e.samples
}

func (e SGCollectTaskEx) Interval() time.Duration {
	return e.interval
}

func (e SGCollectTaskEx) Timeout() time.Duration {
	return e.timeout
}

func (e SGCollectTaskEx) Header() string {
	if e.noHeader {
		return ""
	}
	return e.SGCollectTask.Header()
}

func Sample(t SGCollectTask, samples int, interval time.Duration) SGCollectTaskEx {
	if ex, ok := t.(SGCollectTaskEx); ok {
		ex.samples = samples
		ex.interval = interval
		return ex
	}
	return SGCollectTaskEx{
		SGCollectTask: t,
		samples:       samples,
		interval:      interval,
	}
}

func Timeout(t SGCollectTask, timeout time.Duration) SGCollectTaskEx {
	if ex, ok := t.(SGCollectTaskEx); ok {
		ex.timeout = timeout
		return ex
	}
	return SGCollectTaskEx{
		SGCollectTask: t,
		timeout:       timeout,
	}
}

func Privileged(t SGCollectTask) SGCollectTaskEx {
	if ex, ok := t.(SGCollectTaskEx); ok {
		ex.root = true
		return ex
	}
	return SGCollectTaskEx{
		SGCollectTask: t,
		root:          true,
	}
}

func OverrideOutput(t SGCollectTask, out string) SGCollectTaskEx {
	if ex, ok := t.(SGCollectTaskEx); ok {
		ex.outputFile = out
		return ex
	}
	return SGCollectTaskEx{
		SGCollectTask: t,
		outputFile:    out,
	}
}

func NoHeader(t SGCollectTask) SGCollectTaskEx {
	if ex, ok := t.(SGCollectTaskEx); ok {
		ex.noHeader = true
		return ex
	}
	return SGCollectTaskEx{
		SGCollectTask: t,
		noHeader:      true,
	}
}

// MayFail marks this task as possibly failing in tests. This has no effect at runtime.
func MayFail(t SGCollectTask) SGCollectTaskEx {
	if ex, ok := t.(SGCollectTaskEx); ok {
		ex.mayFailTest = true
		return ex
	}
	return SGCollectTaskEx{
		SGCollectTask: t,
		mayFailTest:   true,
	}
}

type URLTask struct {
	name    string
	url     string
	timeout *time.Duration
}

func (c *URLTask) Name() string {
	return c.name
}

func (c *URLTask) Header() string {
	return c.url
}

func (c *URLTask) Run(ctx context.Context, opts *SGCollectOptions, out io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return fmt.Errorf("failed to build HTTP request: %w", err)
	}
	req.SetBasicAuth(opts.SyncGatewayUsername, string(opts.SyncGatewayPassword))

	client := *getHTTPClient(opts)
	if c.timeout != nil {
		client.Timeout = *c.timeout
	}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to request: %w", err)
	}
	defer res.Body.Close()

	// If the result is JSON, pretty-print it
	if res.Header.Get("Content-Type") == "application/json" {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("failed to load response: %w", err)
		}
		var buf bytes.Buffer
		err = json.Indent(&buf, body, "", "\t")
		if err != nil {
			log.Printf("WARN %s [%s] - failed to pretty-print JSON: %v", c.Name(), c.Header(), err)
			_, err = out.Write(body)
			return err
		}
		_, err = buf.WriteTo(out)
		return err
	}
	_, err = io.Copy(out, res.Body)
	if err != nil {
		return fmt.Errorf("failed to load response: %w", err)
	}
	return nil
}

type FileTask struct {
	name      string
	inputFile string
}

func (f *FileTask) Name() string {
	return f.name
}

func (f *FileTask) Header() string {
	return f.inputFile
}

func (f *FileTask) Run(ctx context.Context, opts *SGCollectOptions, out io.Writer) error {
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

	_, err = io.Copy(out, unzipper) //nolint:gosec - we're copying our own files
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

func (o *OSCommandTask) Run(ctx context.Context, opts *SGCollectOptions, out io.Writer) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	cmd := exec.Command("sh", "-c", o.command) //nolint:gosec
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
		_, err := io.Copy(out, stdout)
		if err != nil {
			log.Printf("WARN %s [%s]: stdout copier error %v", o.name, o.Header(), err)
		}
	}()
	go func() {
		defer wg.Done()
		_, err := io.Copy(out, stderr)
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
// "unix", "linux, "windows", "darwin", "solaris", or "" for all platforms.
func OSTask(platform string, name, cmd string) SGCollectTask {
	switch platform {
	case "":
		return &OSCommandTask{name, cmd, ""}
	case "linux", "windows", "darwin":
		return SGCollectTaskEx{
			SGCollectTask: &OSCommandTask{name, cmd, ""},
			platforms:     []string{platform},
		}
	case "unix":
		return SGCollectTaskEx{
			SGCollectTask: &OSCommandTask{name, cmd, ""},
			platforms:     []string{"linux", "darwin", "freebsd", "netbsd", "openbsd"},
		}
	case "solaris":
		return SGCollectTaskEx{
			SGCollectTask: &OSCommandTask{name, cmd, ""},
			platforms:     []string{"illumos", "solaris"},
		}
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

func (s SGCollectOptionsTask) Run(ctx context.Context, opts *SGCollectOptions, out io.Writer) error {
	_, err := fmt.Fprintf(out, "%#v\n", opts)
	return err
}

type RawStringTask struct {
	name string
	val  string
}

func (s RawStringTask) Name() string {
	return s.name
}

func (s RawStringTask) Header() string {
	return ""
}

func (s RawStringTask) Run(ctx context.Context, opts *SGCollectOptions, out io.Writer) error {
	_, err := fmt.Fprintf(out, "%s\n", s.val)
	return err
}
