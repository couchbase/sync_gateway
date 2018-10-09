package sync_gateway

import (
	"log"

	"github.com/couchbaselabs/gocbconnstr"
)

type GatewayBootstrapConfig struct {
	GoCBConnstr string // A "connection spec" string as expected by couchbaselabs/gocbconnstr
	CBUsername  string // The Couchbase Username to connect as
	CBPassword  string // The password corresponding to CBUsername
	Uuid        string // A uuid to uniquely identify this gateway, which would override the default mechanism
	PortOffset  int    // If non-zero, the listening ports will be offset by this amount.  Eg, if "2" then 4984 -> 4986
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
