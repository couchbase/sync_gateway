//go:build windows
// +build windows

/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package main

import (
	"log"
	"os/exec"

	"os"

	"github.com/kardianos/service"
)

const installLocation = "C:\\Program Files\\Couchbase\\Sync Gateway\\"
const defaultLogFilePath = installLocation + "var\\lib\\couchbase\\logs"

var logger service.Logger
var infoLogger systemLoggerError

type program struct {
	ExePath     string
	ConfigPath  string
	SyncGateway *exec.Cmd
}

func (p *program) Start(s service.Service) error {
	err := p.startup()
	if err != nil {
		return err
	}
	go p.wait()
	return nil
}

func (p *program) startup() error {
	logger.Infof("Starting Sync Gateway service using command: `%s --defaultLogFilePath %s %s`", p.ExePath, defaultLogFilePath, p.ConfigPath)

	if p.ConfigPath != "" {
		p.SyncGateway = exec.Command(p.ExePath, "--defaultLogFilePath", defaultLogFilePath, p.ConfigPath)
	} else {
		p.SyncGateway = exec.Command(p.ExePath, "--defaultLogFilePath", defaultLogFilePath)
	}

	p.SyncGateway.Stdout = infoLogger
	p.SyncGateway.Stderr = infoLogger

	err := p.SyncGateway.Start()

	if err != nil {
		logger.Errorf("Failed to start Sync Gateway due to error %v", err)
		return err
	}
	return nil
}

func (p *program) wait() {
	err := p.SyncGateway.Wait()
	if err != nil {
		logger.Errorf("Sync Gateway exiting with %v", err)
	}
	panic("Sync Gateway service exited.")
}

func (p *program) Stop(s service.Service) error {
	logger.Infof("Stopping Sync Gateway service...")
	p.SyncGateway.Process.Kill()
	return nil
}

func main() {
	svcConfig := &service.Config{
		Name:        "SyncGateway",
		DisplayName: "Couchbase Sync Gateway",
		Description: "Couchbase Sync Gateway mobile application REST gateway service.",
	}

	var exePath string
	var configPath string

	switch len(os.Args) {
	case 2:
		exePath = installLocation + "sync_gateway.exe" // Uses default binary image path
		svcConfig.Arguments = []string{"start"}        // Uses the default config
	case 3:
		exePath = installLocation + "sync_gateway.exe" // Uses default binary image path
		configPath = os.Args[2]                        // Uses custom config
		svcConfig.Arguments = []string{"start", configPath}
	case 4:
		exePath = os.Args[2]    // Uses custom binary image path
		configPath = os.Args[3] // Uses custom config
		svcConfig.Arguments = []string{"start", exePath, configPath}
	case 5:
		exePath = os.Args[2]    // Uses custom binary image path
		configPath = os.Args[3] // Uses custom config
		svcConfig.Arguments = []string{"start", exePath, configPath}
	default:
		panic("Valid parameters combinations are: COMMAND [none, custom config path, or custom exe path and custom config path].")
	}

	prg := &program{
		ExePath:    exePath,
		ConfigPath: configPath,
	}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}
	logger, err = s.Logger(nil)
	if err != nil {
		log.Fatal(err)
	}

	systemLogger, err := s.SystemLogger(nil)
	if err != nil {
		log.Fatalf("Unable to setup system logger: %v", err)
	}

	infoLogger = systemLoggerError{
		systemLogger: systemLogger,
	}

	switch {
	case os.Args[1] == "install":
		logger.Info("Installing Sync Gateway")
		err := s.Install()
		if err != nil {
			logger.Errorf("Failed to install Sync Gateway service: %s", err)
		}
		return
	case os.Args[1] == "uninstall":
		logger.Info("Uninstalling Sync Gateway")
		err := s.Uninstall()
		if err != nil {
			logger.Errorf("Failed to uninstall Sync Gateway service: %s", err)
		}
		return
	case os.Args[1] == "stop":
		err := s.Stop()
		if err != nil {
			logger.Errorf("Failed to stop Sync Gateway service: %s", err)
		}
		return
	case os.Args[1] == "restart":
		err := s.Restart()
		if err != nil {
			logger.Errorf("Failed to restart Sync Gateway service: %s", err)
		}
		return
	}

	err = s.Run()

	if err != nil {
		logger.Error(err)
	}
	logger.Infof("Exiting Sync Gateway service.")
}

type systemLoggerError struct {
	systemLogger service.Logger
}

func (n systemLoggerError) Write(b []byte) (int, error) {
	n.systemLogger.Info(string(b))
	return len(b), nil
}
