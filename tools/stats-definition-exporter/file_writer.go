// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	DefaultFilePath = "./"
	DefaultFileName = "sync_gateway_metrics_metadata"

	JsonFileName = DefaultFileName + ".json"

	TarFileExtension  = ".tar"
	GzipFileExtension = ".gz"
)

func getOutputFileName() string {
	if buildPlaceholderVersionBuildNumberString[0] != '@' {
		return DefaultFileName + "_" + buildPlaceholderVersionBuildNumberString
	}

	return DefaultFileName
}

func writeToFile(stats []StatDefinition) error {
	jsonFilePath := DefaultFilePath + JsonFileName
	err := outputJSON(jsonFilePath, stats)
	if err != nil {
		return fmt.Errorf("could not write json: %w", err)
	}

	// Add to archive
	tarFilePath := DefaultFilePath + getOutputFileName() + TarFileExtension
	err = outputTar(jsonFilePath, tarFilePath)
	if err != nil {
		return fmt.Errorf("could not add to tar: %w", err)
	}

	gzipFilePath := tarFilePath + GzipFileExtension
	err = outputGzip(tarFilePath, gzipFilePath)
	if err != nil {
		return fmt.Errorf("could not add to gzip: %w", err)
	}

	// Cleanup the temporary files
	err = removeFile(tarFilePath)
	if err != nil {
		return fmt.Errorf("could not add to tar: %w", err)
	}

	err = removeFile(jsonFilePath)
	if err != nil {
		return fmt.Errorf("could not add to tar: %w", err)
	}

	return nil
}

func getJSONBytes(stats []StatDefinition) ([]byte, error) {
	statsJSON, err := json.MarshalIndent(stats, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("could not get json bytes: %w", err)
	}

	return statsJSON, nil
}

func outputJSON(fullOutputPath string, stats []StatDefinition) error {
	statsJSON, err := getJSONBytes(stats)
	if err != nil {
		return fmt.Errorf("could not write json to file: %w", err)
	}

	err = os.WriteFile(fullOutputPath, statsJSON, 0644)
	if err != nil {
		return fmt.Errorf("could not write json to file: %w", err)
	}

	return nil
}

func outputTar(fileToAdd string, fullOutputPath string) error {
	tarFile, err := os.Create(fullOutputPath)
	if err != nil {
		return fmt.Errorf("could not create tar file: %w", err)
	}
	defer tarFile.Close()

	tarWriter := tar.NewWriter(tarFile)
	defer tarWriter.Close()

	// Make sure file exists and check if not a directory
	info, err := os.Stat(fileToAdd)
	if err != nil {
		return fmt.Errorf("could not info of file to add: %w", err)
	}

	if info.IsDir() {
		return fmt.Errorf("file to add cannot be a directory: %v", fileToAdd)
	}

	header, err := tar.FileInfoHeader(info, info.Name())
	if err != nil {
		return fmt.Errorf("could not get file to add info: %w", err)
	}

	if err = tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("could not get write header to tar: %w", err)
	}

	file, err := os.Open(fileToAdd)
	if err != nil {
		return fmt.Errorf("could not get open file to add: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(tarWriter, file)
	if err != nil {
		return fmt.Errorf("could not write file to add to tar: %w", err)
	}

	return nil
}

func outputGzip(fileToAdd string, fullOutputPath string) error {
	fileToAddReader, err := os.Open(fileToAdd)
	if err != nil {
		return fmt.Errorf("could not open file to add: %w", err)
	}

	gzipWriter, err := os.Create(fullOutputPath)
	if err != nil {
		return fmt.Errorf("could not create gzip file: %w", err)
	}
	defer gzipWriter.Close()

	archiver := gzip.NewWriter(gzipWriter)
	archiver.Name = filepath.Base(fullOutputPath)
	defer archiver.Close()

	_, err = io.Copy(archiver, fileToAddReader)
	if err != nil {
		return fmt.Errorf("could not copy file to add to gzip file: %w", err)
	}

	return nil
}

func removeFile(filePath string) error {
	err := os.Remove(filePath)
	if err != nil {
		return fmt.Errorf("could not remove file %q: %w", filePath, err)
	}

	return nil
}
