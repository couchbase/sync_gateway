//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUserFunctionAllow(t *testing.T) {
	substitute := map[string]string{
		"args.CITY":  "Paris",
		"args.BREAD": "Baguette",
		"wow":        "huzzah",
		"1234":       "Onetwothreefour",
	}

	replacer := func(pattern string) (string, error) {
		if replacement, ok := substitute[pattern]; ok {
			return replacement, nil
		} else {
			return "", fmt.Errorf("Unknown %q", pattern)
		}
	}

	result, err := DollarSubstitute("vanilla text without dollar signs", replacer)
	assert.NoError(t, err)
	assert.Equal(t, result, "vanilla text without dollar signs")

	// Braced:
	result, err = DollarSubstitute("${args.CITY}", replacer)
	assert.NoError(t, err)
	assert.Equal(t, "Paris", result)

	result, err = DollarSubstitute("sales-${args.CITY}-all", replacer)
	assert.NoError(t, err)
	assert.Equal(t, "sales-Paris-all", result)

	result, err = DollarSubstitute("sales${args.CITY}All", replacer)
	assert.NoError(t, err)
	assert.Equal(t, "salesParisAll", result)

	result, err = DollarSubstitute("sales${args.CITY}${args.BREAD}", replacer)
	assert.NoError(t, err)
	assert.Equal(t, "salesParisBaguette", result)

	// Non-braced:
	result, err = DollarSubstitute("$wow", replacer)
	assert.NoError(t, err)
	assert.Equal(t, "huzzah", result)

	result, err = DollarSubstitute("Say $wow!", replacer)
	assert.NoError(t, err)
	assert.Equal(t, "Say huzzah!", result)

	result, err = DollarSubstitute("($1234)", replacer)
	assert.NoError(t, err)
	assert.Equal(t, "(Onetwothreefour)", result)

	// Escaped `\$`:
	result, err = DollarSubstitute(`expen\$ive`, replacer)
	assert.NoError(t, err)
	assert.Equal(t, "expen$ive", result)
	result, err = DollarSubstitute(`\$wow`, replacer)
	assert.NoError(t, err)
	assert.Equal(t, "$wow", result)
	result, err = DollarSubstitute(`\${wow}`, replacer)
	assert.NoError(t, err)
	assert.Equal(t, "${wow}", result)

	anyReplacer := func(pattern string) (string, error) {
		return "XXX", nil
	}

	// errors:
	_, err = DollarSubstitute("foobar$", anyReplacer)
	assert.Error(t, err)
	_, err = DollarSubstitute("$ {wow}", anyReplacer)
	assert.Error(t, err)
	_, err = DollarSubstitute("knows-${args.CITY", anyReplacer)
	assert.Error(t, err)
	_, err = DollarSubstitute("knows-${args.CITY-${args.CITY}", anyReplacer)
	assert.Error(t, err)
}
