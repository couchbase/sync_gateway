// +build windows

package main

import (
	"log"
	"os/exec"

	"os"

	"github.com/kardianos/service"
)

const installLocation = "C:\\Program Files (x86)\\Couchbase\\"
const defaultLogFilePath = installLocation + "var\\lib\\couchbase\\logs"

var logger service.Logger

type program struct {
	ExePath     string
	ConfigPath  string
	StderrPath  string
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

	f, err := os.OpenFile(p.StderrPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		logger.Warningf("Failed to open std err %q: %v", p.StderrPath, err)
		return err
	}
	defer f.Close()
	p.SyncGateway.Stderr = f

	err = p.SyncGateway.Start()
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
	var stderrPath string

	switch len(os.Args) {
	case 2:
		exePath = installLocation + "sync_gateway.exe"             // Uses default binary image path
		stderrPath = defaultLogFilePath + "sync_gateway_error.log" // Uses default stderr path
		svcConfig.Arguments = []string{"start", stderrPath}        // Uses the default config
	case 3:
		exePath = installLocation + "sync_gateway.exe"             // Uses default binary image path
		configPath = os.Args[2]                                    // Uses custom config
		stderrPath = defaultLogFilePath + "sync_gateway_error.log" // Uses default stderr path
		svcConfig.Arguments = []string{"start", configPath, stderrPath}
	case 4:
		exePath = os.Args[2]                                       // Uses custom binary image path
		configPath = os.Args[3]                                    // Uses custom config
		stderrPath = defaultLogFilePath + "sync_gateway_error.log" // Uses default stderr path
		svcConfig.Arguments = []string{"start", exePath, configPath, stderrPath}
	case 5:
		exePath = os.Args[2]    // Uses custom binary image path
		configPath = os.Args[3] // Uses custom config
		stderrPath = os.Args[4] // Uses custom stderr path
		svcConfig.Arguments = []string{"start", exePath, configPath, stderrPath}
	default:
		panic("Valid parameters combinations are: COMMAND [none, custom config path, or custom exe path and custom config path].")
	}

	prg := &program{
		ExePath:    exePath,
		ConfigPath: configPath,
		StderrPath: stderrPath,
	}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}
	logger, err = s.Logger(nil)
	if err != nil {
		log.Fatal(err)
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
