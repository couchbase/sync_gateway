//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/mmcloughlin/geohash"
)

// Applies a template to a message using the "state" model, producing a document body.
func applyStateTemplate(template Body, payload any, timestamp time.Time, topic TopicMatch) (Body, error) {
	if template != nil {
		tmpl := templater{
			payload:   payload,
			timestamp: timestamp,
			topic:     topic}
		doc, _ := tmpl.apply(template).(Body)
		return doc, tmpl.err
	} else {
		// Default template:
		return Body{"payload": payload}, nil
	}
}

// Validates a map as a template for the "state" model.
func validateStateTemplate(template Body) error {
	if template != nil {
		tmpl := templater{
			payload:                Body{},
			timestamp:              time.Now(),
			allowMissingProperties: true}
		tmpl.apply(template)
		if tmpl.err != nil {
			return tmpl.err
		}
	}
	return nil
}

// Applies a template to a message using the "time_series" model.
// The first item of the result array will be a timestamp.
func applyTimeSeriesTemplate(config *TimeSeriesConfig, payload any, timestamp time.Time, allowMissingProperties bool) ([]any, error) {
	if config.ValuesTemplate != nil {
		tmpl := templater{
			payload:                payload,
			timestamp:              timestamp,
			allowMissingProperties: allowMissingProperties}
		array := tmpl.apply(config.ValuesTemplate).([]any)
		if tmpl.err != nil {
			return nil, tmpl.err
		}

		if config.TimeProperty != "" && timestamp.Unix() != 0 {
			timestamp = tmpl.expandTimestamp(config)
			if tmpl.err != nil {
				return nil, tmpl.err
			}
			if u := timestamp.Unix(); u < minValidTimestamp {
				return nil, fmt.Errorf("message timestamp is too far in the past: %v", timestamp)
			} else if u > maxValidTimestamp {
				return nil, fmt.Errorf("message timestamp is too far in the future: %v", timestamp)
			}
		}

		array = slices.Insert(array, 0, any(timestamp.Unix()))
		return array, nil

	} else {
		// Default template:
		return []any{timestamp.Unix(), payload}, nil
	}
}

// Applies a template to a message using the "time_series" model.
// The first two items of the result array will be a geohash string and a timestamp.
func applySpaceTimeSeriesTemplate(config *SpaceTimeSeriesConfig, payload any, timestamp time.Time, allowMissingProperties bool) ([]any, error) {
	entry, err := applyTimeSeriesTemplate(&config.TimeSeriesConfig, payload, timestamp, allowMissingProperties)
	if err == nil {
		// Apply the Latitude/Longitude templates:
		tmpl := templater{
			payload:                payload,
			timestamp:              timestamp,
			allowMissingProperties: allowMissingProperties}
		coord := tmpl.apply([]any{config.Latitude, config.Longitude}).([]any)
		if tmpl.err != nil {
			return nil, tmpl.err
		} else if len(coord) != 2 {
			if !allowMissingProperties {
				return nil, fmt.Errorf("invalid latitude or longitude values")
			}
		} else if lat, err := asFloat64(coord[0]); err != nil {
			if !allowMissingProperties {
				return nil, err
			}
		} else if lon, err := asFloat64(coord[1]); err != nil {
			if !allowMissingProperties {
				return nil, err
			}
		} else {
			var gh any = geohash.Encode(lat, lon)
			entry = slices.Insert(entry, 0, gh)
		}
	}
	return entry, err
}

var errNonNumeric = fmt.Errorf("value is non-numeric")

func asFloat64(n any) (float64, error) { //TODO: Is there already a utility for this?
	switch n := n.(type) {
	case int64:
		return float64(n), nil
	case float64:
		return n, nil
	case json.Number:
		return n.Float64()
	default:
		return 0, errNonNumeric
	}
}

func isNumber(n any) bool {
	_, err := asFloat64(n)
	return err == nil
}
