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
	err = tr.setupSGCollectLog()
	if err != nil {
		return nil, err
	}
	return tr, nil
}

func (tr *TaskRunner) Finalize() {
	log.Println("Task runner finalizing...")
	for _, fd := range tr.files {
		err := fd.Close()
		if err != nil {
			log.Printf("Failed to close %s: %v", fd.Name(), err)
		}
	}
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

	run := func() {
		defer func() {
			if panicked := recover(); panicked != nil {
				log.Printf("PANIC - %s [%s]: %v", task.Name(), task.Header(), panicked)
			}
		}()
		ctx := context.Background()
		if to := tex.Timeout(); to > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, to)
			defer cancel()
		}
		log.Printf("RUN - %s [%s]", task.Name(), task.Header())
		name := task.Name()
		_ = name
		err := task.Run(ctx, tr.opts, fd)
		if err != nil {
			log.Printf("FAIL - %q [%s]: %v", task.Name(), task.Header(), err)
			_, _ = fmt.Fprintln(fd, err.Error())
			return
		}
		log.Printf("OK - %s [%s]", task.Name(), task.Header())
	}

	if tex.NumSamples() > 0 {
		for i := 0; i < tex.NumSamples(); i++ {
			run()
			if i != tex.NumSamples()-1 {
				log.Printf("Taking sample %d of %q [%s] after %v seconds", i+2, task.Name(), task.Header(), tex.Interval())
				time.Sleep(tex.Interval())
			}
		}
	} else {
		run()
	}
	_, err := fd.WriteString("\n")
	if err != nil {
		log.Printf("WARN %s [%s] - failed to write closing newline: %v", task.Name(), task.Header(), err)
	}
}
