//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var badPlusRe = regexp.MustCompile(`[^/]\+|\+[^/]`) // matches `+` next to a non-`/``

// An MQTT topic filter -- a topic string that may contain wildcards `+` and `#`.
type TopicFilter struct {
	pattern *regexp.Regexp // Equivalent RE, or nil if filter contains no wildcards
	literal string         // Filter itself, if it has no wildcards, else empty
	value   any            // Used by TopicMap to store associated values on its filters
}

// Compiles an MQTT topic filter string.
func MakeTopicFilter(topicPattern string) (filter TopicFilter, err error) {
	err = filter.Init(topicPattern)
	return
}

func (filter *TopicFilter) Init(topicPattern string) error {
	if !strings.ContainsAny(topicPattern, "+#") {
		filter.literal = topicPattern
		return nil
	}

	if badPlusRe.MatchString(topicPattern) {
		return fmt.Errorf("invalid use of `+` wildcard in topic pattern %q", topicPattern)
	}

	pat := regexp.QuoteMeta(topicPattern)
	pat = strings.ReplaceAll(pat, `\+`, `([^/]+)`)

	if pat == "#" {
		pat = "(.*)"
	} else if strings.HasSuffix(pat, "/#") {
		pat = pat[0:len(pat)-2] + `(?:/(.+))?`
	} else if strings.Contains(pat, "#") {
		return fmt.Errorf("invalid use of `#` wildcard in topic pattern %q", topicPattern)
	}

	re, err := regexp.Compile(`^` + pat + `$`)
	filter.pattern = re
	return err
}

// Returns true if this filter matches the topic.
func (tf *TopicFilter) Matches(topic string) bool {
	if tf.pattern != nil {
		return tf.pattern.MatchString(topic)
	} else {
		return tf.literal == topic
	}
}

// If this filter matches the topic, returns an array of submatch strings, one for each wildcard
// (`+` or `#`) in the filter. If the filter has no wildcards, the array will be empty.
// If the filter doesn't match, returns nil.
func (tf *TopicFilter) FindStringSubmatch(topic string) []string {
	if tf.pattern != nil {
		if match := tf.pattern.FindStringSubmatch(topic); match != nil {
			return match[1:]
		}
	} else {
		if tf.literal == topic {
			return []string{}
		}
	}
	return nil
}

//======== TOPIC MAP:

// A mapping from MQTT topic filters to arbitrary values.
type TopicMap[V any] struct {
	filters []TopicFilter
}

// Creates an empty TopicMap, preallocating space for `capacity` filters.
func NewTopicMap[V any](capacity int) TopicMap[V] {
	return TopicMap[V]{filters: make([]TopicFilter, 0, capacity)}
}

// Creates a TopicMap from a map, interpreting the keys as topic filters.
func MakeTopicMap[V any](m map[string]V) (tm TopicMap[V], err error) {
	for pattern, val := range m {
		if err = tm.AddFilter(pattern, val); err != nil {
			return
		}
	}
	return
}

// Adds a single topic filter and value.
func (tm *TopicMap[V]) AddFilter(pattern string, val V) error {
	f, err := MakeTopicFilter(pattern)
	if err == nil {
		f.value = val
		tm.filters = append(tm.filters, f)
	}
	return err
}

// Matches a topic name to a filter and returns the associated value.
// The first filter that matches is used.
func (tm *TopicMap[V]) Get(topic string) (val V) {
	for _, tf := range tm.filters {
		if tf.Matches(topic) {
			return tf.value.(V)
		}
	}
	return
}

// Matches a topic name to a filter and returns the associated value,
// plus the strings matched by any wildcards.
// The first filter that matches is used.
func (tm *TopicMap[V]) Match(topic string) (val V, match *TopicMatch) {
	for _, tf := range tm.filters {
		if matches := tf.FindStringSubmatch(topic); matches != nil {
			return tf.value.(V), &TopicMatch{Name: topic, Wildcards: matches}
		}
	}
	return
}

//======== TOPICMATCH:

// The result of successfully matching a topic against a TopicMap.
type TopicMatch struct {
	Name      string   // The matched topic name
	Wildcards []string // Values for the wildcards in the filter string
}

var kTemplateTopicMatchRegexp = regexp.MustCompile(`^\$(\d+)$`)

// If `pattern` is of the form `$n`, and `n` is a valid 1-based index into the array of
// wildcards, returns the corresponding wildcard match.
func (tm *TopicMatch) ExpandPattern(pattern string) (string, error) {
	if !strings.HasPrefix(pattern, "$") {
		return pattern, nil
	}
	if match := kTemplateTopicMatchRegexp.FindStringSubmatch(pattern); match != nil {
		return tm.ExpandNumberPattern(match[1])
	}
	return "", fmt.Errorf("invalid topic wildcard substitution string %q", pattern)
}

// If `pattern` is of the form `$n`, and `n` is a valid 1-based index into the array of
// wildcards, returns the corresponding wildcard match.
func (tm *TopicMatch) ExpandNumberPattern(pattern string) (string, error) {
	if n, err := strconv.ParseUint(pattern, 10, 32); err != nil || n < 1 {
		return "", fmt.Errorf("invalid topic wildcard substitution string $%s", pattern)
	} else if int(n) > len(tm.Wildcards) {
		return "", fmt.Errorf("$%d is invalid: the topic filter only has %d wildcard(s)", n, len(tm.Wildcards))
	} else {
		return tm.Wildcards[n-1], nil
	}
}
