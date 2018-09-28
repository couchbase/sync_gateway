package cmd

import (
	"github.com/couchbase/sync_gateway/rest"
)

func BootstrapConfigFromParams() (rest.BootstrapConfig, error) {

	config, err := rest.NewGatewayBootstrapConfig(GoCBConnstr)
	if err != nil {
		return rest.BootstrapConfig{}, err
	}

	config.PortOffset = PortOffset
	config.CBUsername = CBUsername
	config.CBPassword = CBPassword

	return *config, nil

}
