package cmd

import (
	"github.com/couchbase/sync_gateway"
)

func BootstrapConfigFromParams() (sync_gateway.GatewayBootstrapConfig, error) {

	config, err := sync_gateway.NewGatewayBootstrapConfig(GoCBConnstr)
	if err != nil {
		return sync_gateway.GatewayBootstrapConfig{}, err
	}

	config.PortOffset = PortOffset
	config.CBUsername = CBUsername
	config.CBPassword = CBPassword

	return *config, nil

}
