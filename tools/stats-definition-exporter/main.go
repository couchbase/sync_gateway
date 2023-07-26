// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/couchbase/sync_gateway/base"
)

// Substituted by Jenkins build scripts
const buildPlaceholderVersionBuildNumberString = "@PRODUCT_VERSION@"

func main() {
	outputConsoleOnlyFlag := flag.Bool("no-file", false, "Output stat metadata to console (stdout) only.")
	flag.Parse()

	logger := log.New(os.Stderr, "", 0)

	stats, err := getStats(logger)
	if err != nil {
		logger.Fatalf("could not get stats: %v", err)
	}

	if *outputConsoleOnlyFlag {
		err = statsToConsole(logger, stats)
	} else {
		err = statsToFile(logger, stats, DefaultFilePath)
	}
	if err != nil {
		logger.Fatalf("could not write stats: %v", err)
	}
}

func getStats(logger *log.Logger) ([]StatDefinition, error) {
	globalStats, dbStats, err := registerStats()
	if err != nil {
		return nil, fmt.Errorf("could not register stats: %w", err)
	}

	// Append the db stat definitions on to the global stat definitions
	stats := traverseAndRetrieveStats(logger, globalStats)
	stats = append(stats, traverseAndRetrieveStats(logger, dbStats)...)

	return stats, nil
}

func statsToFile(logger *log.Logger, stats []StatDefinition, filePath string) error {
	err := writeToFile(stats, filePath)
	if err != nil {
		return fmt.Errorf("could not write stat definitions to a file: %w", err)
	}

	return nil
}

func statsToConsole(logger *log.Logger, stats []StatDefinition) error {
	json, err := getJSONBytes(stats)
	if err != nil {
		return fmt.Errorf("could not get JSON bytes: %w", err)
	}

	fmt.Printf("%s", json)

	return nil
}

func registerStats() (*base.GlobalStat, *base.DbStats, error) {
	// Don't register stats with Prometheus
	base.SkipPrometheusStatsRegistration = true

	sgStats, err := base.NewSyncGatewayStats()
	if err != nil {
		return nil, nil, fmt.Errorf("could not create sg stats: %w", err)
	}

	dbStats, err := sgStats.NewDBStats("", true, true, false, []string{""}, []string{""})
	if err != nil {
		return nil, nil, fmt.Errorf("could not create db stats: %w", err)
	}

	// Replicator stats only get initialized when a replication is started usually
	_, err = dbStats.DBReplicatorStats("")
	if err != nil {
		return nil, nil, fmt.Errorf("could not create db replicator stats: %w", err)
	}

	return sgStats.GlobalStats, dbStats, nil
}
