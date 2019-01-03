package rest

import (
	"fmt"
	"github.com/couchbase/mobile-service"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/gocbconnstr"
	msgrpc "github.com/couchbase/mobile-service/mobile_service_grpc"
	"google.golang.org/grpc"
	pkgerrors "github.com/pkg/errors"

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

func (c *BootstrapConfig) NewMobileServiceAdminClient() (adminClient msgrpc.MobileServiceAdminClient, err error) {

	mobileSvcHostPort, err := c.ChooseActiveMobileSvcGrpcEndpoint(ChooseMobileSvcFirst)
	if err != nil {
		return nil, err
	}

	// Set up a connection to the server.
	conn, err := grpc.Dial(mobileSvcHostPort, grpc.WithInsecure())
	if err != nil {
		return nil, pkgerrors.Wrapf(err, "Error connecting via grpc to mobile service")
	}

	adminClient = msgrpc.NewMobileServiceAdminClient(conn)
	return adminClient, nil

}


func (c *BootstrapConfig) ChooseActiveMobileSvcGrpcEndpoint(strategy ChooseMobileSvcStrategy) (mobileSvcHostPort string, err error) {

	mobileServiceNodes, err := c.ActiveMobileSvcGrpcEndpoints()
	if err != nil {
		return "", err
	}

	if len(mobileServiceNodes) == 0 {
		return "", fmt.Errorf("Cannot find any active mobile service nodes to connect to")
	}

	switch strategy {
	case ChooseMobileSvcFirst:
		return mobileServiceNodes[0], nil
	case ChooseMobileSvcRandom:
		return mobileServiceNodes[base.RandIntRange(0, len(mobileServiceNodes))], nil
	default:
		return "", fmt.Errorf("Unknown strategy: %v", strategy)
	}

}

func (c *BootstrapConfig) ActiveMobileSvcGrpcEndpoints() (mobileSvcHostPorts []string, err error) {

	// Get the raw connection spec from the bootstrap config
	connSpec, err := gocbconnstr.Parse(c.GoCBConnstr)
	if err != nil {
		return []string{}, pkgerrors.Wrapf(err, "Error parsing connection string")
	}

	// Filter out cb server addresses that don't give 200 responses to /pools/default,
	// which probably means they have been removed from the cluster.
	connSpec = base.FilterAddressesInCluster(connSpec, c.CBUsername, c.CBPassword)

	// Calculate the GRPC endpoint port based on the couchbase server port and the PortGrpcTlsOffset
	mobileSvcHostPorts = []string{}
	for _, address := range connSpec.Addresses {
		grpcTargetPort := mobile_service.PortGrpcTlsOffset + address.Port
		hostPort := fmt.Sprintf("%s:%d", address.Host, grpcTargetPort)
		mobileSvcHostPorts = append(mobileSvcHostPorts, hostPort)
	}

	return mobileSvcHostPorts, nil

}