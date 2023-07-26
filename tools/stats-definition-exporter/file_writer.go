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
	"fmt"
	"os"
)

func writeToFile(stats []StatDefinition, fullFilePath string) error {
	statsJSON, err := getJSONBytes(stats)
	if err != nil {
		return fmt.Errorf("could not get json bytes: %w", err)
	}

	err = os.WriteFile(fullFilePath, statsJSON, 0644)
	if err != nil {
		return fmt.Errorf("could not write json to file: %w", err)
	}

	return nil
}

func getJSONBytes(stats []StatDefinition) ([]byte, error) {
	statsJSON, err := json.MarshalIndent(stats, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("could not marshal json: %w", err)
	}

	return statsJSON, nil
}
