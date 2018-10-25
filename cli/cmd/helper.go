package cmd

import (
	"github.com/couchbase/sync_gateway/rest"
)

func BootstrapConfigFromParams() (rest.GatewayBootstrapConfig, error) {

	config, err := rest.NewGatewayBootstrapConfig(GoCBConnstr)
	if err != nil {
		return rest.GatewayBootstrapConfig{}, err
	}

	config.PortOffset = PortOffset
	config.CBUsername = CBUsername
	config.CBPassword = CBPassword

	return *config, nil

}
