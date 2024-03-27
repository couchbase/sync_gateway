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
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	minValidTimestamp = int64(1712329445) // April 2024, roughly
	maxValidTimestamp = int64(9999999999)
)

//TODO: Substitute `$` anywhere in a value, not just the entire value! 4/12/24
//TODO: Support `[n]` for array indexing
//TODO: Support some type of transformer/filter, i.e. to format date from int->ISO8601

// Applies a template to a message
type templater struct {
	payload                any        // The message payload; any parsed JSON type
	timestamp              time.Time  // The time of the message
	topic                  TopicMatch // Topic name and any matched wildcards
	allowMissingProperties bool       // If true, missing properties in patterns aren't an error
	err                    error      // The error, if any
}

// Recursively applies the template
func (tmpl *templater) apply(template any) any {
	if tmpl.err != nil {
		return nil
	}
	switch val := template.(type) {
	case map[string]any:
		result := make(Body, len(val))
		for k, v := range val {
			if a := tmpl.apply(v); a != nil {
				result[k] = a
			}
		}
		return result
	case []any:
		result := make([]any, 0, len(val))
		for _, v := range val {
			if a := tmpl.apply(v); a != nil {
				result = append(result, a)
			}
		}
		return result
	case string:
		return tmpl.expand(val)
	default:
		return template
	}
}

var filterRE = regexp.MustCompile(`\s*\|\s*`)

// Transforms `$`-prefixed patterns in the input string, using `base.DollarSubstitute`.
// Values are substituted from the message payload or a timestamp.
// If a pattern expands to something other than a string, there can't be anything else in the
// input string. So if the `world` property of the payload were an array, then
// "Hello ${message.payload.world}" would produce an error but "${message.payload.world}" would
// be OK and would return the array.
func (tmpl *templater) expand(input string) any {
	if strings.IndexByte(input, '$') < 0 {
		return input
	}

	var nonStringResult any
	var nonStringParam string
	stringResult, err := base.DollarSubstitute(input, func(param string) (string, error) {
		components := filterRE.Split(param, -1)

		result, err := tmpl.expandMatch(components[0])

		for _, filter := range components[1:] {
			if err == nil {
				result, err = applyFilter(result, filter)
			}
		}

		if err != nil {
			return "", err
		} else if resultStr, ok := result.(string); ok {
			return resultStr, nil
		} else {
			nonStringResult = result
			nonStringParam = param
			return "", nil
		}
	})

	if nonStringParam == "" {
		nonStringResult = any(stringResult)
	} else if stringResult != "" {
		err = fmt.Errorf("in the template %q, the parameter %q expands to a non-string", input, nonStringParam)
	}
	if err != nil {
		tmpl.err = err
	}
	return nonStringResult
}

// Given the contents of a `${...}` pattern, returns the value it resolves to.
func (tmpl *templater) expandMatch(param string) (any, error) {
	matches := strings.Split(param, ".")
	if len(matches) == 1 {
		switch matches[0] {
		case "now":
			// $now defaults to numeric timestamp (Unix epoch):
			return tmpl.timestamp.Unix(), nil
		default:
			// Is it a number? In that case, subsitute from the TopicMatch:
			if n, err := strconv.ParseUint(matches[0], 10, 32); err == nil && n >= 1 {
				if int(n) <= len(tmpl.topic.Wildcards) {
					return tmpl.topic.Wildcards[n-1], nil
				} else {
					return "", fmt.Errorf("$%d is invalid: the topic filter only has %d wildcard(s)", n, len(tmpl.topic.Wildcards))
				}
			}
		}

	} else if matches[0] == "now" {
		switch matches[1] {
		case "iso8601":
			// Insert ISO-8601 timestamp:
			if date, err := tmpl.timestamp.MarshalText(); err == nil {
				return string(date), nil
			} else {
				return "", err
			}
		case "unix":
			// Insert numeric timestamp (Unix epoch):
			return tmpl.timestamp.Unix(), nil
		}

	} else if matches[0] == "message" {
		switch matches[1] {
		case "topic":
			return tmpl.topic.Name, nil
		case "payload":
			if tmpl.payload == nil {
				return nil, nil
			}
			root := map[string]any{"payload": tmpl.payload}
			reflected, err := base.EvalKeyPath(root, strings.Join(matches[1:], "."), tmpl.allowMissingProperties)
			if err != nil {
				return nil, err
			} else if reflected.IsValid() {
				return reflected.Interface(), nil
			} else {
				return nil, nil
			}
		}
	}
	// Give up:
	return "", fmt.Errorf("unknown template pattern ${%s}", param)
}

// Processes an arbitrary value with a filter.
func applyFilter(v any, filter string) (any, error) {
	switch filter {
	case "json": // Encodes the value as a JSON string, unless it's nil
		if v == nil {
			return v, nil
		}
		j, err := base.JSONMarshal(v)
		return string(j), err
	case "required": // Produces an error if the value is nil
		if v == nil {
			return nil, fmt.Errorf("missing or invalid property")
		} else {
			return v, nil
		}
	case "number": // Converts the value to a number, parsing strings and turning true/false to 0/1
		if isNumber(v) {
			return v, nil // already a number
		} else if b, ok := v.(bool); ok {
			if b {
				return 1, nil
			} else {
				return 0, nil
			}
		} else if str, ok := v.(string); ok {
			if i, err := strconv.ParseInt(str, 10, 64); err == nil {
				return i, nil
			} else {
				return strconv.ParseFloat(str, 64)
			}
		} else {
			return nil, nil
		}
	case "round": // Rounds a float to the nearest integer; leaves anything else alone.
		switch n := v.(type) {
		case float64:
			return int64(math.Round(n)), nil
		case json.Number:
			if i, err := n.Int64(); err == nil {
				return i, nil // leave ints alone to avoid roundoff error
			} else if f, err := n.Float64(); err == nil {
				return int64(math.Round(f)), nil
			}
		}
		return v, nil
	case "string": // Converts the value to a string. Like `json` except it leaves strings alone.
		if _, ok := v.(string); ok {
			return v, nil
		} else if v == nil {
			return v, nil
		} else {
			j, err := base.JSONMarshal(v)
			return string(j), err
		}
	case "trim": // Trims whitespace from a string.
		if str, ok := v.(string); ok {
			return strings.TrimSpace(str), nil
		} else {
			return v, nil
		}
	default:
		return nil, fmt.Errorf("unknown filter `|%s`", filter)
	}
}

func (tmpl *templater) expandTimestamp(config *TimeSeriesConfig) (result time.Time) {
	if config.TimeProperty == "" {
		return tmpl.timestamp
	}
	timeVal := tmpl.expand(config.TimeProperty)
	if tmpl.err != nil {
		return
	}
	if timeVal == nil {
		tmpl.err = fmt.Errorf("payload is missing time property %s", config.TimeProperty)
		return
	}
	switch config.TimeFormat {
	case "iso8601", "":
		if timeStr, ok := timeVal.(string); ok {
			result, tmpl.err = time.Parse(time.RFC3339, timeStr)
		} else {
			tmpl.err = fmt.Errorf("timestamp is not a string, cannot be parsed as iso8601")
		}
	case "unix_epoch":
		if timeInt, ok := timeVal.(int64); ok {
			result = time.Unix(timeInt, 0)
		} else if timeFloat, ok := timeVal.(float64); ok {
			secs := math.Floor(timeFloat)
			nanos := (timeFloat - secs) * 1e9
			result = time.Unix(int64(secs), int64(math.Round(nanos)))
		} else {
			tmpl.err = fmt.Errorf("timestamp is not a number, cannot be parsed as unix_epoch")
		}
	default:
		tmpl.err = fmt.Errorf("invalid time-series `time_format` %q", config.TimeFormat)
	}
	return
}
