//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package document

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateProofOfAttachment(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	attData := []byte(`hello world`)

	nonce, proof1, err := GenerateProofOfAttachment(attData)
	require.NoError(t, err)
	assert.True(t, len(nonce) >= 20, "nonce should be at least 20 bytes")
	assert.NotEmpty(t, proof1)
	assert.True(t, strings.HasPrefix(proof1, "sha1-"))

	proof2 := ProveAttachment(attData, nonce)
	assert.NotEmpty(t, proof1, "")
	assert.True(t, strings.HasPrefix(proof1, "sha1-"))

	assert.Equal(t, proof1, proof2, "GenerateProofOfAttachment and ProveAttachment produced different proofs.")
}

func TestDecodeAttachmentError(t *testing.T) {
	attr, err := DecodeAttachment(make([]int, 1))
	assert.Nil(t, attr, "Attachment of data (type []int) should not get decoded.")
	assert.Error(t, err, "It should throw 400 invalid attachment data (type []int)")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	attr, err = DecodeAttachment(make([]float64, 1))
	assert.Nil(t, attr, "Attachment of data (type []float64) should not get decoded.")
	assert.Error(t, err, "It should throw 400 invalid attachment data (type []float64)")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	attr, err = DecodeAttachment(make([]string, 1))
	assert.Nil(t, attr, "Attachment of data (type []string) should not get decoded.")
	assert.Error(t, err, "It should throw 400 invalid attachment data (type []string)")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	attr, err = DecodeAttachment(make(map[string]int, 1))
	assert.Nil(t, attr, "Attachment of data (type map[string]int) should not get decoded.")
	assert.Error(t, err, "It should throw 400 invalid attachment data (type map[string]int)")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	attr, err = DecodeAttachment(make(map[string]float64, 1))
	assert.Nil(t, attr, "Attachment of data (type map[string]float64) should not get decoded.")
	assert.Error(t, err, "It should throw 400 invalid attachment data (type map[string]float64)")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	attr, err = DecodeAttachment(make(map[string]string, 1))
	assert.Error(t, err, "should throw 400 invalid attachment data (type map[string]float64)")
	assert.Error(t, err, "It 400 invalid attachment data (type map[string]string)")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	book := struct {
		author string
		title  string
		price  float64
	}{author: "William Shakespeare", title: "Hamlet", price: 7.99}
	attr, err = DecodeAttachment(book)
	assert.Nil(t, attr)
	assert.Error(t, err, "It should throw 400 invalid attachment data (type struct { author string; title string; price float64 })")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))
}

func TestGetAttVersion(t *testing.T) {
	var tests = []struct {
		name                    string
		inputAttVersion         interface{}
		expectedValidAttVersion bool
		expectedAttVersion      int
	}{
		{"int attachment version", AttVersion2, true, AttVersion2},
		{"float64 attachment version", float64(AttVersion2), true, AttVersion2},
		{"invalid json.Number attachment version", json.Number("foo"), false, 0},
		{"valid json.Number attachment version", json.Number(strconv.Itoa(AttVersion2)), true, AttVersion2},
		{"invaid string attachment version", strconv.Itoa(AttVersion2), false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := map[string]interface{}{"ver": tt.inputAttVersion}
			version, ok := GetAttachmentVersion(meta)
			assert.Equal(t, tt.expectedValidAttVersion, ok)
			assert.Equal(t, tt.expectedAttVersion, version)
		})
	}
}
