/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSystemDataRedact(t *testing.T) {
	clusterName := "My Super Secret IP"
	systemdata := SystemData(clusterName)

	RedactSystemData = true
	assert.Equal(t, systemDataPrefix+clusterName+systemDataSuffix, systemdata.Redact())

	RedactSystemData = false
	assert.Equal(t, clusterName, systemdata.Redact())
}

func TestSD(t *testing.T) {
	RedactSystemData = true
	defer func() { RedactSystemData = false }()

	// Base string test
	sd := SD("hello world")
	assert.Equal(t, systemDataPrefix+"hello world"+systemDataSuffix, sd.Redact())

	// Big Int
	sd = SD(big.NewInt(1234))
	assert.Equal(t, systemDataPrefix+"1234"+systemDataSuffix, sd.Redact())

	// Struct
	sd = SD(struct{}{})
	assert.Equal(t, systemDataPrefix+"{}"+systemDataSuffix, sd.Redact())

	// String slice
	sd = SD([]string{"hello", "world", "o/"})
	assert.Equal(t, "[ "+systemDataPrefix+"hello"+systemDataSuffix+" "+systemDataPrefix+"world"+systemDataSuffix+" "+systemDataPrefix+"o/"+systemDataSuffix+" ]", sd.Redact())

	// Set
	sd = SD(SetOf("hello", "world"))
	// As a set comes from a map we can't be sure which order it'll end up with so should check both permutations
	redactedPerm1 := "{" + systemDataPrefix + "hello" + systemDataSuffix + ", " + systemDataPrefix + "world" + systemDataSuffix + "}"
	redactedPerm2 := "{" + systemDataPrefix + "world" + systemDataSuffix + ", " + systemDataPrefix + "hello" + systemDataSuffix + "}"
	redactedSet := sd.Redact()
	redactedCorrectly := redactedPerm1 == redactedSet || redactedPerm2 == redactedSet
	assert.True(t, redactedCorrectly, "Unexpected redact got %v", redactedSet)
}
