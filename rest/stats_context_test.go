/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNetworkInterfaceStatsForHostnamePort(t *testing.T) {

	_, err := networkInterfaceStatsForHostnamePort("127.0.0.1:4984")
	assert.NoError(t, err, "Unexpected Error")

	_, err = networkInterfaceStatsForHostnamePort("localhost:4984")
	assert.NoError(t, err, "Unexpected Error")

	_, err = networkInterfaceStatsForHostnamePort("0.0.0.0:4984")
	assert.NoError(t, err, "Unexpected Error")

	_, err = networkInterfaceStatsForHostnamePort(":4984")
	assert.NoError(t, err, "Unexpected Error")

}
