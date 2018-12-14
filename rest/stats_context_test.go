package rest

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNetworkInterfaceStatsForHostnamePort(t *testing.T) {

	_, err := networkInterfaceStatsForHostnamePort("127.0.0.1:4984")
	assert.NoError(t, err, "Unexpected Erorr")

	_, err = networkInterfaceStatsForHostnamePort("localhost:4984")
	assert.NoError(t, err, "Unexpected Erorr")

	_, err = networkInterfaceStatsForHostnamePort("0.0.0.0:4984")
	assert.NoError(t, err, "Unexpected Erorr")

	_, err = networkInterfaceStatsForHostnamePort(":4984")
	assert.NoError(t, err, "Unexpected Erorr")

}