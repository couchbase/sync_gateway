package db

import (
	"encoding/binary"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestParseXattr(t *testing.T) {
	zeroByte := byte(0)
	// Build payload for single xattr pair and body
	xattrKey := "_sync"
	xattrValue := `{"seq":1}`
	xattrPairLength := 4 + len(xattrKey) + len(xattrValue) + 2
	xattrLength := xattrPairLength + 4
	body := `{"value":"ABC"}`

	// Build up the dcp Body
	dcpBody := make([]byte, 8)
	binary.BigEndian.PutUint32(dcpBody[0:4], uint32(xattrLength))
	binary.BigEndian.PutUint32(dcpBody[4:8], uint32(xattrPairLength))
	dcpBody = append(dcpBody, xattrKey...)
	dcpBody = append(dcpBody, zeroByte)
	dcpBody = append(dcpBody, xattrValue...)
	dcpBody = append(dcpBody, zeroByte)
	dcpBody = append(dcpBody, body...)

	resultBody, resultXattr, err := parseXattrStreamData("_sync", dcpBody)
	assertNoError(t, err, "Unexpected error parsing dcp body")
	assert.Equals(t, string(resultBody), string(body))
	assert.Equals(t, string(resultXattr), string(xattrValue))

	// Attempt to retrieve non-existent xattr
	resultBody, resultXattr, err = parseXattrStreamData("nonexistent", dcpBody)
	assertNoError(t, err, "Unexpected error parsing dcp body")
	assert.Equals(t, string(resultBody), string(body))
	assert.Equals(t, string(resultXattr), "")
}
