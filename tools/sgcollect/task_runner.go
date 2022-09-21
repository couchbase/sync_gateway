package main

import (
	"context"
	"fmt"
	"io"
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
	err = tr.setupSGCollectLog()
	if err != nil {
		return nil, err
	}
	return tr, nil
}

func (tr *TaskRunner) Finalize() {
	for _, fd := range tr.files {
		err := fd.Close()
		if err != nil {
			log.Printf("Failed to close %s: %v", fd.Name(), err)
		}
	}
}

// setupSGCollectLog will redirect the standard library log package's output to both stderr and a log file in the temporary directory.
func (tr *TaskRunner) setupSGCollectLog() error {
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
	return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
}

func (tr *TaskRunner) writeHeader(w io.Writer, task SGCollectTask) error {
	separator := strings.Repeat("=", 78)
	// example:
	// ==============================================================================
	// Collect server status
	// *main.URLTask: http://127.0.0.1:4985/_status
	// ==============================================================================
	_, err := fmt.Fprintf(w, "%s\n%s\n%T: %s\n%s\n", separator, task.Name(), task, task.Header(), separator)
	return err
}

func (tr *TaskRunner) Run(task SGCollectTask) {
	tex := TaskEx(task)
	// TODO: opportunity to parallelise here - one worker per output file
	if !tex.ShouldRun(runtime.GOOS) {
		log.Printf("Skipping %q on %s.", task.Name(), runtime.GOOS)
		return
	}
	if tex.RequiresRoot() {
		uid := os.Getuid()
		if uid != -1 && uid != 0 {
			log.Printf("Skipping %q - requires root privileges.", task.Name())
			return
		}
	}
	outputFile := task.OutputFile()
	if outputFile == "" {
		outputFile = defaultOutputFile
	}
	fd, ok := tr.files[outputFile]
	if !ok {
		var err error
		fd, err = os.OpenFile(filepath.Join(tr.tmpDir, outputFile), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
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

	run := func() {
		ctx := context.Background()
		if to := tex.Timeout(); to > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, to)
			defer cancel()
		}
		err := task.Run(ctx, tr.opts, fd)
		if err != nil {
			log.Printf("FAILed to run %q [%s]: %v", task.Name(), task.Header(), err)
			_, _ = fmt.Fprintln(fd, err.Error())
			return
		}
		log.Printf("OK - %s [%s]", task.Name(), task.Header())
	}

	if tex.NumSamples() > 0 {
		for i := 0; i < tex.NumSamples(); i++ {
			log.Printf("Taking sample %d of %q [%s] after %v seconds", i+1, task.Name(), task.Header(), tex.Interval())
			run()
			time.Sleep(tex.Interval())
		}
	} else {
		run()
	}
	_, err := fd.WriteString("\n\n")
	if err != nil {
		log.Printf("WARN %s [%s] - failed to write closing newline: %v", task.Name(), task.Header(), err)
	}
}
