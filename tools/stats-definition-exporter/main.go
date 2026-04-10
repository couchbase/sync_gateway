// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"os"

	"github.com/couchbase/sync_gateway/base"
)

const (
	DefaultFullFilePath    = "./metrics_metadata.json"
	DefaultDashboardPath   = "./grafana-dashboard.json"
	FormatMetadata         = "metadata"
	FormatSupportalGrafana = "supportal-grafana"
	FormatCapellaGrafana   = "capella-grafana"
)

func main() {
	outputConsoleOnlyFlag := flag.Bool("no-file", false, "Output stat metadata to console (stdout) only.")
	outputFileFlag := flag.String("output", "", "Full file path of outputted JSON file. Defaults to metrics_metadata.json for metadata format or grafana-dashboard.json for grafana formats.")
	formatFlag := flag.String("format", FormatMetadata, "Output format: metadata, supportal-grafana, or capella-grafana.")
	flag.Parse()

	logger := log.New(os.Stderr, "", 0)

	// Validate format
	switch *formatFlag {
	case FormatMetadata, FormatSupportalGrafana, FormatCapellaGrafana:
		// Valid format
	default:
		logger.Fatalf("invalid format %q: must be one of %s, %s, or %s", *formatFlag, FormatMetadata, FormatSupportalGrafana, FormatCapellaGrafana)
	}

	// Set default output file based on format
	outputFile := outputFileFlag
	if *outputFileFlag == "" {
		defaultPath := DefaultFullFilePath
		if *formatFlag != FormatMetadata {
			defaultPath = DefaultDashboardPath
		}
		outputFile = &defaultPath
	}

	if *outputConsoleOnlyFlag {
		outputFile = nil
	}

	err := statsToFile(logger, outputFile, *formatFlag)
	if err != nil {
		logger.Fatalf("%v", err)
	}
}

// Write stats to outputFile unless nil, in which case write to stdout
func statsToFile(logger *log.Logger, outputFile *string, format string) error {
	stats, err := getStats(logger)
	if err != nil {
		return fmt.Errorf("could not get stats: %w", err)
	}

	// Write to console if outputFile is nil
	writer := os.Stdout
	if outputFile != nil {
		file, err := os.Create(*outputFile)
		if err != nil {
			return fmt.Errorf("could not create file: %w", err)
		}

		defer closeAndLogError(logger, file)

		writer = file
	}

	switch format {
	case FormatSupportalGrafana:
		err = writeGrafanaDashboard(stats, supportalConfig, writer)
	case FormatCapellaGrafana:
		err = writeGrafanaDashboard(stats, capellaConfig, writer)
	default:
		err = writeStats(stats, writer)
	}
	if err != nil {
		return fmt.Errorf("could not write stats: %w", err)
	}

	return nil
}

func closeAndLogError(logger *log.Logger, c io.Closer) {
	err := c.Close()
	if err != nil {
		logger.Fatalf("could not close file: %v", err)
	}
}

func getStats(logger *log.Logger) (StatDefinitions, error) {
	globalStats, dbStats, err := registerStats()
	if err != nil {
		return nil, fmt.Errorf("could not register stats: %w", err)
	}

	// Get all the stats
	globalStatsDefinitions := traverseAndRetrieveStats(logger, globalStats)
	dbStatDefinitions := traverseAndRetrieveStats(logger, dbStats)

	// Merge the two maps
	stats := make(StatDefinitions, len(globalStatsDefinitions)+len(dbStatDefinitions))
	maps.Copy(stats, globalStatsDefinitions)
	maps.Copy(stats, dbStatDefinitions)

	return stats, nil
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

func writeStats(stats StatDefinitions, writer io.Writer) error {
	encoder := json.NewEncoder(writer)

	encoder.SetIndent("", "\t")
	err := encoder.Encode(stats)
	if err != nil {
		return fmt.Errorf("could not encode stats: %w", err)
	}

	return nil
}
