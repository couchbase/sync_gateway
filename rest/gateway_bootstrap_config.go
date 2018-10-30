package rest

import (
	"log"

	"github.com/couchbaselabs/gocbconnstr"
)

// A container for all of the bootstrap configuration parameters
type BootstrapConfig struct {
	GoCBConnstr string // A "connection spec" string as expected by couchbaselabs/gocbconnstr
	CBUsername  string // The Couchbase Username to connect as
	CBPassword  string // The password corresponding to CBUsername
	Uuid        string // A uuid to uniquely identify this gateway, which would override the default mechanism
	PortOffset  int    // If non-zero, the listening ports will be offset by this amount.  Eg, if "2" then 4984 -> 4986
}

func NewGatewayBootstrapConfig(connStr string) (config *BootstrapConfig, err error) {
	config = &BootstrapConfig{
		GoCBConnstr: connStr,
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return config, nil
}

func (c *BootstrapConfig) Validate() error {
	_, err := gocbconnstr.Parse(c.GoCBConnstr)
	if err != nil {
		return err
	}
	return nil
}
