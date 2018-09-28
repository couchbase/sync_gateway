package main

import (
	"fmt"

	"github.com/couchbase/sync_gateway"
)

func RunGateway(pushStats bool) {

	// Client setup
	gw := sync_gateway.NewGateway()
	defer gw.Close()

	// Load snapshot of configuration from MetaKV
	gw.LoadConfigSnapshot()

	// Kick off goroutine to observe stream of metakv changes
	go func() {
		err := gw.ObserveMetaKVChanges(sync_gateway.AddTrailingSlash(sync_gateway.MOBILE))
		if err != nil {
			panic(fmt.Sprintf("Error observing metakv changes: %v", err))
		}
	}()

	// Kick off http/s server
	go gw.RunServer()

	if pushStats {
		// Push stats (blocks)
		if err := gw.PushStatsStream(); err != nil {
			panic(fmt.Sprintf("Error pushing stats: %v", err))
		}
	} else {
		select {}
	}

}

func main() {

	RunGateway(true)

}
