package sync_gateway

import (
	"github.com/couchbaselabs/gocbconnstr"
	"log"
)

type GatewayBootstrapConfig struct {

	GoCBConnstr string // A "connection spec" string as expected by couchbaselabs/gocbconnstr

	CBUsername string  // The Couchbase Username to connect as

	CBPassword string  // The password corresponding to CBUsername


}

func NewGatewayBootstrapConfig(connStr string) (config *GatewayBootstrapConfig, err error) {
	config = &GatewayBootstrapConfig{
		GoCBConnstr: connStr,
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return config, nil
}



func (c *GatewayBootstrapConfig) Validate() error {
	connSpec, err := gocbconnstr.Parse(c.GoCBConnstr)
	if err != nil {
		return err
	}
	log.Printf("connSpec: %+v", connSpec)
	return nil
}

