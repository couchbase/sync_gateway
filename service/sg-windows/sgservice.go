package main

import (
	"log"
	"os/exec"

	"github.com/kardianos/service"
	"os"
)

var logger service.Logger

type program struct{
	ConfigPath  string
	SyncGateway *exec.Cmd
}

func (p *program) Start(s service.Service) error {
	go p.run()
	return nil
}
func (p *program) run() {
	logger.Info("Starting Sync Gateway service...")
	p.SyncGateway = exec.Command("C:\\Program Files (x86)\\Couchbase\\sync_gateway.exe", p.ConfigPath)
	err := p.SyncGateway.Start()
	if err != nil {
		logger.Errorf("Failed to start Sync Gateway due to error %v", err)
		return
	}
	completed := make(chan error, 1)
	go func() {
		completed <- p.SyncGateway.Wait()
	}()
	select {
	case err := <- completed:
		if err != nil {
			logger.Errorf("Sync Gateway exitting with status = %v", err)
		}
	}
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
		Description: "Couchbase Sync Gateway mobile application gateway service.",
	}

	if len(os.Args) > 1 {
		configPath := os.Args[1]
		svcConfig.Arguments = []string { configPath }
	}

	prg := &program{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}
	logger, err = s.Logger(nil)
	if err != nil {
		log.Fatal(err)
	}
	err = s.Run()

	if err != nil {
		logger.Error(err)
	}
	logger.Infof("Exiting Sync Gateway service.")
}
