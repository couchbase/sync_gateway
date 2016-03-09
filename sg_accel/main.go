//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/couchbase/cbgt"
	cbgtRest "github.com/couchbase/cbgt/rest"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/gorilla/mux"
)

// Simple Sync Gateway launcher tool.
func main() {

	signalchannel := make(chan os.Signal, 1)
	signal.Notify(signalchannel, syscall.SIGHUP)

	go func() {
		for range signalchannel {
			base.Logf("SIGHUP: Reloading Config....\n")
			rest.ReloadConf()
		}
	}()

	ServerMain()

}

func ServerMain() {
	rest.ParseCommandLine()
	rest.ReloadConf()
	serverConfig := rest.GetConfig()
	ValidateConfigOrPanic(serverConfig)
	RunServer(serverConfig)
}

func ValidateConfigOrPanic(config *rest.ServerConfig) {

	// if index writer != true for any databases, panic
	if rest.GetConfig().HasAnyIndexReaderConfiguredDatabases() {
		base.LogPanic("SG is running in sg-accelerator mode but there are databases configured as index readers")
	}
}

// Starts and runs the server given its configuration. (This function never returns.)
func RunServer(config *rest.ServerConfig) {
	rest.PrettyPrint = config.Pretty

	base.Logf("==== %s ====", rest.LongVersionString)

	if os.Getenv("GOMAXPROCS") == "" && runtime.GOMAXPROCS(0) == 1 {
		cpus := runtime.NumCPU()
		if cpus > 1 {
			runtime.GOMAXPROCS(cpus)
			base.Logf("Configured Go to use all %d CPUs; setenv GOMAXPROCS to override this", cpus)
		}
	}

	base.SetMaxFileDescriptors(*config.MaxFileDescriptors)

	ac := NewSGAccelContext(config)

	if rest.GetConfig().ProfileInterface != nil {
		//runtime.MemProfileRate = 10 * 1024
		base.Logf("Starting profile server on %s", *config.ProfileInterface)
		go func() {
			http.ListenAndServe(*config.ProfileInterface, nil)
		}()
	}

	base.Logf("Starting admin server on %s", *config.AdminInterface)
	router := rest.CreateAdminRouter(ac.serverContext)
	addCbgtRoutes(router, ac)
	handler := rest.CreateAdminHandlerForRouter(ac.serverContext, router)

	go config.Serve(*config.AdminInterface, handler)
}

func addCbgtRoutes(router *mux.Router, ac *SGAccelContext) error {

	subrouter := router.PathPrefix("/_cbgt").Subrouter()

	versionString := "0.0.0"
	staticDir := ""
	staticETag := ""
	var messageRing *cbgt.MsgRing
	assetDir := cbgtRest.AssetDir
	asset := cbgtRest.Asset

	cbgtManager := ac.cbgtContext.Manager
	_, _, err := cbgtRest.InitRESTRouter(
		subrouter,
		versionString,
		cbgtManager,
		staticDir,
		staticETag,
		messageRing,
		assetDir,
		asset,
	)
	if err != nil {
		return err
	}
	return nil
}
