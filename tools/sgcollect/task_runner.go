package main

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const defaultOutputFile = "sync_gateway.log"

type TaskRunner struct {
	tmpDir    string
	startTime time.Time
	files     map[string]*os.File
	opts      *SGCollectOptions
}

func NewTaskRunner(opts *SGCollectOptions) (*TaskRunner, error) {
	tr := &TaskRunner{
		startTime: time.Now(),
		files:     make(map[string]*os.File),
		opts:      opts,
	}
	var err error
	tr.tmpDir, err = os.MkdirTemp(opts.TmpDir, fmt.Sprintf("sgcollect_info-%s-*", tr.startTime.Format("2006-01-02T15:04:05Z07")))
	if err != nil {
		return nil, fmt.Errorf("could not use temporary dir: %w", err)
	}
	log.Printf("Using temporary directory %s", tr.tmpDir)
	return tr, nil
}

func (tr *TaskRunner) Finalize() {
	log.Println("Task runner finalizing...")
	log.SetOutput(os.Stderr)
	for _, fd := range tr.files {
		err := fd.Close()
		if err != nil {
			log.Printf("Failed to close %s: %v", fd.Name(), err)
		}
	}
}

func (tr *TaskRunner) Cleanup() error {
	return os.RemoveAll(tr.tmpDir)
}

func (tr *TaskRunner) ZipResults(outputPath string, prefix string, copier CopyFunc) error {
	fd, err := os.OpenFile(outputPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reate output: %w", err)
	}
	defer func(fd *os.File) {
		err := fd.Close()
		if err != nil {
			log.Printf("WARN: failed to close unredacted output file: %v", err)
		}
	}(fd)

	zw := zip.NewWriter(fd)
	defer func(zw *zip.Writer) {
		err := zw.Close()
		if err != nil {
			log.Printf("WARN: failed to close unredacted output zipper: %v", err)
		}
	}(zw)

	err = filepath.WalkDir(tr.tmpDir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		// Returning a non-nil error will stop the walker completely - we want to capture as much as we can.
		fileFd, err := os.Open(path)
		if err != nil {
			log.Printf("WARN: failed to open %s: %v", path, err)
			return nil
		}
		defer fileFd.Close()

		zipPath := prefix + string(os.PathSeparator) + strings.TrimPrefix(path, tr.tmpDir+string(os.PathSeparator)) // TODO: properly remove prefix
		zipFile, err := zw.Create(zipPath)
		if err != nil {
			log.Printf("WARN: failed to open %s in zip: %v", zipPath, err)
			return nil
		}
		_, err = copier(zipFile, fileFd)
		if err != nil {
			log.Printf("WARN: failed to copy to %s in zip: %v", zipPath, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walker error: %w", err)
	}
	return nil
}

// SetupSGCollectLog will redirect the standard library log package's output to both stderr and a log file in the temporary directory.
// After calling this, make sure to call Finalize, which will undo the change.
func (tr *TaskRunner) SetupSGCollectLog() error {
	fd, err := tr.createFile("sgcollect_info.log")
	if err != nil {
		return fmt.Errorf("failed to create sgcollect_info.log: %w", err)
	}
	tr.files["sgcollect_info.log"] = fd
	log.SetOutput(io.MultiWriter(os.Stderr, fd))
	return nil
}

func (tr *TaskRunner) createFile(name string) (*os.File, error) {
	path := filepath.Join(tr.tmpDir, name)
	return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
}

func (tr *TaskRunner) writeHeader(w io.Writer, task SGCollectTask) error {
	separator := strings.Repeat("=", 78)
	// example:
	// ==============================================================================
	// Collect server status
	// main.SGCollectTaskEx (main.URLTask): http://127.0.0.1:4985/_status
	// ==============================================================================
	var err error
	if tex, ok := task.(SGCollectTaskEx); ok {
		_, err = fmt.Fprintf(w, "%s\n%s\n%T (%T): %s\n%s\n", separator, task.Name(), task, tex.SGCollectTask, task.Header(), separator)
	} else {
		_, err = fmt.Fprintf(w, "%s\n%s\n%T: %s\n%s\n", separator, task.Name(), task, task.Header(), separator)
	}
	return err
}

func (tr *TaskRunner) Run(task SGCollectTask) {
	tex := TaskEx(task)

	outputFile := tex.outputFile
	if outputFile == "" {
		outputFile = defaultOutputFile
	}
	fd, ok := tr.files[outputFile]
	if !ok {
		var err error
		fd, err = tr.createFile(outputFile)
		if err != nil {
			log.Printf("FAILed to run %q - failed to create file: %v", task.Name(), err)
			return
		}
		tr.files[outputFile] = fd
	}

	if header := task.Header(); header != "" {
		err := tr.writeHeader(fd, task)
		if err != nil {
			log.Printf("FAILed to run %q - failed to write header: %v", task.Name(), err)
			return
		}
	}

	res := ExecuteTask(tex, tr.opts, fd, func(s string) {
		log.Println(s)
	}, false)
	if res.SkipReason != "" {
		log.Printf("SKIP %s [%s] - %s", task.Name(), task.Header(), res.SkipReason)
		_, _ = fmt.Fprintf(fd, "skipped - %s", res.SkipReason)
	}
	if res.Error != nil {
		log.Printf("FAIL %s [%s] - %v", task.Name(), task.Header(), res.Error)
		_, _ = fmt.Fprintf(fd, "%s", res.Error)
	}

	_, err := fd.WriteString("\n")
	if err != nil {
		log.Printf("WARN %s [%s] - failed to write closing newline: %v", task.Name(), task.Header(), err)
	}
}

type TaskExecutionResult struct {
	Task       SGCollectTask
	SkipReason string
	Error      error
}

func ExecuteTask(task SGCollectTask, opts *SGCollectOptions, output io.Writer, log func(string), failFast bool) TaskExecutionResult {
	tex := TaskEx(task)
	if !tex.ShouldRun(runtime.GOOS) {
		return TaskExecutionResult{
			Task:       task,
			SkipReason: fmt.Sprintf("not executing on platform %s", runtime.GOOS),
		}
	}
	if tex.RequiresRoot() {
		uid := os.Getuid()
		if uid != -1 && uid != 0 {
			return TaskExecutionResult{
				Task:       task,
				SkipReason: "requires root privileges",
			}
		}
	}

	run := func() (err error) {
		defer func() {
			if panicked := recover(); panicked != nil {
				if recErr, ok := panicked.(error); ok {
					err = recErr
				} else {
					err = fmt.Errorf("task panic: %v", panicked)
				}
			}
		}()
		ctx := context.Background()
		if to := tex.Timeout(); to > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, to)
			defer cancel()
		}
		log(fmt.Sprintf("RUN %s [%s]", task.Name(), task.Header()))
		return task.Run(ctx, opts, output)
	}

	var err error
	if tex.NumSamples() > 0 {
		for i := 0; i < tex.NumSamples(); i++ {
			err = run()
			if err != nil && failFast {
				return TaskExecutionResult{
					Task:  task,
					Error: err,
				}
			}
			if i != tex.NumSamples()-1 {
				log(fmt.Sprintf("Taking sample %d of %q [%s] after %v seconds", i+2, task.Name(), task.Header(), tex.Interval()))
				time.Sleep(tex.Interval())
			}
		}
	} else {
		err = run()
	}
	return TaskExecutionResult{
		Task:  task,
		Error: err,
	}
}
