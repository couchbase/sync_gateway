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

func TestMetadataRedact(t *testing.T) {
	clusterName := "My Super Secret Cluster"
	metadata := Metadata(clusterName)

	RedactMetadata = true
	assert.Equal(t, metaDataPrefix+clusterName+metaDataSuffix, metadata.Redact())

	RedactMetadata = false
	assert.Equal(t, clusterName, metadata.Redact())
}

func TestMD(t *testing.T) {
	RedactMetadata = true
	defer func() { RedactMetadata = false }()

	//Base string test
	md := MD("hello world")
	assert.Equal(t, metaDataPrefix+"hello world"+metaDataSuffix, md.Redact())

	//Big Int
	md = MD(big.NewInt(1234))
	assert.Equal(t, metaDataPrefix+"1234"+metaDataSuffix, md.Redact())

	//Struct
	md = MD(struct{}{})
	assert.Equal(t, metaDataPrefix+"{}"+metaDataSuffix, md.Redact())

	//String slict
	md = MD([]string{"hello", "world", "o/"})
	assert.Equal(t, "[ "+metaDataPrefix+"hello"+metaDataSuffix+" "+metaDataPrefix+"world"+metaDataSuffix+" "+metaDataPrefix+"o/"+metaDataSuffix+" ]", md.Redact())
}
