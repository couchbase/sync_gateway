package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/mobile-service"
	"github.com/couchbase/mobile-service/mobile_service"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/gocbconnstr"
	pkgerrors "github.com/pkg/errors"
	"google.golang.org/grpc"
)

type Gateway struct {
	Uuid     string // The uuid of this gateway node (the same one used by CBGT and stored in a local file)
	Hostname string // The hostname of this gateway node, not sure where this would come from

	GrpcConn   *grpc.ClientConn
	GrpcClient mobile_service.MobileServiceClient

	// The current configuration state.
	// Key: path in metakv (eg, /mobile/config/databases/db1)
	// Val: a MetaKVPair whose value contains a JSON dictionary
	Config map[string]*mobile_service.MetaKVPair

	// The SG server context
	ServerContext *ServerContext

	// The "bootstrap config" for this gateway to be able to connect to Couchbase Server to get actual config
	BootstrapConfig GatewayBootstrapConfig
}

func NewGateway(bootstrapConfig GatewayBootstrapConfig) *Gateway {
	gw := Gateway{
		BootstrapConfig: bootstrapConfig,
		Uuid:            bootstrapConfig.Uuid,
		Hostname:        GATEWAY_HOSTNAME,
		Config:          map[string]*mobile_service.MetaKVPair{},
	}

	err := gw.ConnectMobileSvc()
	if err != nil {
		panic(fmt.Sprintf("Error connecting to mobile service: %v", err))
	}

	return &gw
}

// Connect to the mobile service and:
//
//   - Push stats on a regular basis
//   - Receive configuration updates
func (gw *Gateway) ConnectMobileSvc() error {

	mobileSvcHostPort, err := gw.ChooseRandomMobileService()
	if err != nil {
		panic(fmt.Sprintf("Error finding mobile service to connect to: %v", err))
	}
	log.Printf("Connecting to mobile service: %v", mobileSvcHostPort)

	// Set up a connection to the server.
	conn, err := grpc.Dial(mobileSvcHostPort, grpc.WithInsecure())
	if err != nil {
		return err
	}
	gw.GrpcConn = conn

	// Create client
	gw.GrpcClient = mobile_service.NewMobileServiceClient(conn)

	return nil

}

func (gw *Gateway) ChooseRandomMobileService() (mobileSvcHostPort string, err error) {

	mobileServiceNodes, err := gw.FindMobileServiceNodes()
	if err != nil {
		return "", err
	}

	return mobileServiceNodes[base.RandIntRange(0, len(mobileServiceNodes))], nil

}

// Since we don't know which mobile service node will be assigned which grpc port, we
// need to generate every combination of server ip and grpc port starting with the known grpc start port.
func (gw *Gateway) FindMobileServiceNodes() (mobileSvcHostPorts []string, err error) {

	connSpec, err := gocbconnstr.Parse(gw.BootstrapConfig.GoCBConnstr)
	if err != nil {
		return []string{}, err
	}

	grpcTargetPorts := []int{}
	for portOffset := 0; portOffset < len(connSpec.Addresses); portOffset++ {
		grpcTargetPort := portOffset + mobile_mds.PortGrpcTls
		grpcTargetPorts = append(grpcTargetPorts, grpcTargetPort)
	}

	mobileSvcHostPorts = []string{}

	for _, address := range connSpec.Addresses {
		for _, grpcTargetPort := range grpcTargetPorts {
			hostPort := fmt.Sprintf("%s:%d", address.Host, grpcTargetPort)
			mobileSvcHostPorts = append(mobileSvcHostPorts, hostPort)
		}
	}

	return mobileSvcHostPorts, nil

}

func (gw *Gateway) Start() error {

	// Load snapshot of configuration from MetaKV
	if err := gw.LoadConfigSnapshot(); err != nil {
		return err
	}

	// Kick off http/s server
	if err := gw.RunServer(); err != nil {
		return err
	}

	// Kick off goroutine to observe stream of metakv changes
	go func() {
		err := gw.ObserveMetaKVChangesRetry(mobile_mds.KeyDirMobileRoot)
		if err != nil {
			base.Warnf(base.KeyAll, fmt.Sprintf("Error observing metakv changes: %v", err))

		}
	}()

	// Kick off goroutine to push stats
	go func() {
		if err := gw.PushStatsStreamWithReconnect(); err != nil {
			base.Warnf(base.KeyAll, fmt.Sprintf("Error pushing stats: %v.  Stats will no longer be pushed.", err))
		}
	}()

	return nil

}


func (gw *Gateway) LoadConfigSnapshot() error {

	// Get snapshot of MetaKV tree
	keyMobileConfig := mobile_mds.KeyDirMobileGateway
	metaKvPairs, err := gw.GrpcClient.MetaKVListAllChildren(context.Background(), &mobile_service.MetaKVPath{Path: keyMobileConfig})
	if err != nil {
		return err
	}
	for _, metakvPair := range metaKvPairs.Items {
		gw.Config[metakvPair.Path] = metakvPair
	}
	return nil
}

func (gw *Gateway) RunServer() error {

	// Parse json stored in metakv into a ServerConfig
	serverConfig, err := gw.LoadServerConfig()
	if err != nil {
		return err
	}

	serverContext := RunServer(serverConfig)

	gw.ServerContext = serverContext

	// This has to be done separately _after_ the call to gw.ServerContext, because
	// without a valid gw.ServerContext, loading databases will fail.
	if err := gw.LoadDbConfigAllDbs(); err != nil {
		return err
	}

	return nil
}

// Dummy method that just blocks forever
func (gw *Gateway) Wait() {
	select {}
}

func (gw *Gateway) Close() {
	gw.GrpcConn.Close()
}

func (gw *Gateway) ObserveMetaKVChangesRetry(path string) error {

	for {
		log.Printf("Starting ObserveMetaKVChanges")
		if err := gw.ObserveMetaKVChanges(path); err != nil {
			log.Printf("ObserveMetaKVChanges returned error: %v, retrying.", err)
			time.Sleep(time.Second * 1)
			gw.ObserveMetaKVChanges(path)
		}

	}

}

func (gw *Gateway) ObserveMetaKVChanges(path string) error {

	stream, err := gw.GrpcClient.MetaKVObserveChildren(context.Background(), &mobile_service.MetaKVPath{Path: path})
	if err != nil {
		return err
	}

	for {
		updatedConfigKV, err := stream.Recv()
		if err != nil {
			return err
		}

		log.Printf("ObserveMetaKVChanges got metakv change for %v: %+v", path, updatedConfigKV)

		if strings.HasPrefix(updatedConfigKV.Path, mobile_mds.KeyMobileGatewayDatabases) {

			if err := gw.ProcessDatabaseMetaKVPair(updatedConfigKV); err != nil {
				return err
			}

		} else if strings.HasPrefix(updatedConfigKV.Path, mobile_mds.KeyMobileGateway) {
			// Server level config updated
			if err := gw.HandleServerConfigUpdated(updatedConfigKV); err != nil {
				return err
			}
		}

	}

}

func (gw *Gateway) ProcessDatabaseMetaKVPair(metakvPair *mobile_service.MetaKVPair) error {
	existingMetaKVConfig, found := gw.Config[metakvPair.Path]
	switch found {
	case true:
		// If we got here, we already know about this db key in the config state
		// Is the db being deleted or updated?
		switch metakvPair.Rev {
		case "":
		case "null":
			// Db is being deleted
			if err := gw.HandleDbDelete(metakvPair, existingMetaKVConfig); err != nil {
				return err
			}
		default:
			// Db is being updated
			log.Printf("updatedDbConfig: %v", string(metakvPair.Value))
			if err := gw.HandleDbUpdate(metakvPair, existingMetaKVConfig); err != nil {
				return err
			}
		}
	case false:
		// We don't know about this db key, brand new
		if err := gw.HandleDbAdd(metakvPair); err != nil {
			return err
		}
	}

	return nil

}

func (gw *Gateway) HandleServerConfigUpdated(metaKvPair *mobile_service.MetaKVPair) error {

	gw.Config[metaKvPair.Path] = metaKvPair

	// TODO: reload server level config

	base.Warnf(base.KeyAll, "HandleServerConfigUpdated ignoring change: %v", metaKvPair.Path)

	return nil

}

func (gw *Gateway) HandleDbDelete(metaKvPair, existingDbConfig *mobile_service.MetaKVPair) error {

	if len(metaKvPair.Value) != 0 {
		return fmt.Errorf("If db config is being deleted, incoming value should be empty")
	}

	// Delete from config map
	delete(gw.Config, metaKvPair.Path)

	// The metakv pair path will be: /mobile/gateway/config/databases/database-1
	// Get the last item from the path and use that as the dbname
	dbName, err := MetaKVLastItemPath(metaKvPair.Path)
	if err != nil {
		return err
	}

	// Make sure the db actually exists
	_, err = gw.ServerContext.GetDatabase(dbName)
	if err != nil {
		return err
	}

	// Remove the database
	removed := gw.ServerContext.RemoveDatabase(dbName)
	if !removed {
		return fmt.Errorf("Could not remove db: %v", metaKvPair.Path)
	}

	return nil
}

func (gw *Gateway) HandleDbAdd(metaKvPair *mobile_service.MetaKVPair) error {

	gw.Config[metaKvPair.Path] = metaKvPair

	dbConfig, err := gw.LoadDbConfig(metaKvPair.Path)
	if err != nil {
		return err
	}

	// The metakv pair path will be: /mobile/gateway/config/databases/database-1
	// Get the last item from the path and use that as the dbname
	dbName, err := MetaKVLastItemPath(metaKvPair.Path)
	if err != nil {
		return err
	}
	dbConfig.Name = dbName

	// Enhance the db config with the connection parameters from the gateway bootstrap config
	dbConfig.Server = &gw.BootstrapConfig.GoCBConnstr
	dbConfig.Username = gw.BootstrapConfig.CBUsername
	dbConfig.Password = gw.BootstrapConfig.CBPassword

	_, err = gw.ServerContext.AddDatabaseFromConfig(dbConfig)
	if err != nil {
		return err
	}

	// TODO: other db add handling
	return nil
}

func (gw *Gateway) HandleDbUpdate(metaKvPair, existingDbConfig *mobile_service.MetaKVPair) error {

	// The metakv pair path will be: /mobile/gateway/config/databases/database-1
	// Get the last item from the path and use that as the dbname
	dbName, err := MetaKVLastItemPath(metaKvPair.Path)
	if err != nil {
		return err
	}

	// Is the database already known?  If so, we must remove it first
	_, err = gw.ServerContext.GetDatabase(dbName)
	if err == nil {
		// Remove the database.  This is a workaround for the fact that servercontext doesn't seem
		// to have a way to update the db config.
		removed := gw.ServerContext.RemoveDatabase(dbName)
		if !removed {
			return fmt.Errorf("Could not remove db: %v", metaKvPair.Path)
		}
	}

	// Re-add DB based on updated config
	return gw.HandleDbAdd(metaKvPair)

}

func (gw *Gateway) LoadServerConfig() (serverConfig *ServerConfig, err error) {

	gotListenerConfig := false
	gotGeneralConfig := false
	serverListenerConfig := ServerConfig{}
	serverGeneralConfig := ServerConfig{}

	//Get the general + listener config
	for _, metaKvPair := range gw.Config {
		switch {
		case metaKvPair.Path == mobile_mds.KeyMobileGatewayListener:
			gotListenerConfig = true
			if err := json.Unmarshal(metaKvPair.Value, &serverListenerConfig); err != nil {
				return nil, pkgerrors.Wrapf(err, "Error unmarshalling")
			}

		case metaKvPair.Path == mobile_mds.KeyMobileGatewayGeneral:
			gotGeneralConfig = true
			if err := json.Unmarshal(metaKvPair.Value, &serverGeneralConfig); err != nil {
				return nil, pkgerrors.Wrapf(err, "Error unmarshalling")
			}
		}

	}

	if !gotListenerConfig {
		return nil, fmt.Errorf("Did not find required MetaKV config: %v", mobile_mds.KeyMobileGatewayListener)
	}

	if !gotGeneralConfig {
		return nil, fmt.Errorf("Did not find required MetaKV config: %v", mobile_mds.KeyMobileGatewayGeneral)
	}

	// Copy over the values from listener into general
	// TODO: figure out a less hacky/brittle way to do this
	if serverListenerConfig.Interface != nil {
		serverGeneralConfig.Interface = serverListenerConfig.Interface
		if gw.BootstrapConfig.PortOffset != 0 {
			*serverGeneralConfig.Interface = ApplyPortOffset(*serverGeneralConfig.Interface, gw.BootstrapConfig.PortOffset)
		}
	}
	if serverListenerConfig.AdminInterface != nil {
		serverGeneralConfig.AdminInterface = serverListenerConfig.AdminInterface
		if gw.BootstrapConfig.PortOffset != 0 {
			*serverGeneralConfig.AdminInterface = ApplyPortOffset(*serverGeneralConfig.AdminInterface, gw.BootstrapConfig.PortOffset)
		}
	}
	if serverListenerConfig.SSLCert != nil {
		serverGeneralConfig.SSLCert = serverListenerConfig.SSLCert
	}
	if serverListenerConfig.SSLKey != nil {
		serverGeneralConfig.SSLKey = serverListenerConfig.SSLKey
	}

	return &serverGeneralConfig, nil

}

func (gw *Gateway) LoadDbConfigAllDbs() error {

	// Second pass to get the databases
	for _, metaKvPair := range gw.Config {
		switch {
		case strings.HasPrefix(metaKvPair.Path, mobile_mds.KeyMobileGatewayDatabases):
			if err := gw.ProcessDatabaseMetaKVPair(metaKvPair); err != nil {
				return err
			}
		}

	}

	return nil

}

func (gw *Gateway) LoadDbConfig(path string) (dbConfig *DbConfig, err error) {

	metaKvPair, found := gw.Config[path]
	if !found {
		return nil, fmt.Errorf("Key not found: %v", path)
	}

	dbConfigTemp := DbConfig{}

	if err := json.Unmarshal(metaKvPair.Value, &dbConfigTemp); err != nil {
		return nil, pkgerrors.Wrapf(err, "Error unmarshalling db config")
	}

	return &dbConfigTemp, nil

}

func (gw *Gateway) PushStatsStreamWithReconnect() error {

	for {

		log.Printf("Starting push stats stream")

		if err := gw.PushStatsStream(); err != nil {
			log.Printf("Error pushing stats: %v.  Retrying", err)
		}

		// TODO: this should be a backoff / retry and only give up after a number of retries
		// TODO: also, this should happen in a central place in the codebase rather than arbitrarily in the PushStatsStream()
		for {
			log.Printf("Attempting to reconnect to grpc server")

			err := gw.ConnectMobileSvc()
			if err != nil {
				log.Printf("Error connecting to grpc server: %v.  Retrying", err)
				time.Sleep(time.Second)
				continue
			}

			break
		}

	}

}

func (gw *Gateway) PushStatsStream() error {

	// Stream stats
	stream, err := gw.GrpcClient.SendStats(context.Background())
	if err != nil {
		return err
	}
	gatewayMeta := mobile_service.Gateway{
		Uuid:     gw.Uuid,
		Hostname: gw.Hostname,
	}
	creds := mobile_service.Creds{
		Username: gw.BootstrapConfig.CBUsername,
		Password: gw.BootstrapConfig.CBPassword,
	}
	stats := []mobile_service.Stats{
		mobile_service.Stats{Gateway: &gatewayMeta, NumChangesFeeds: "10", Creds: &creds},
		mobile_service.Stats{Gateway: &gatewayMeta, NumChangesFeeds: "20", Creds: &creds},
		mobile_service.Stats{Gateway: &gatewayMeta, NumChangesFeeds: "30", Creds: &creds},
	}

	i := 0
	for {

		statIndex := i % len(stats)
		stat := stats[statIndex]
		log.Printf("Sending stat: %v", stat)
		if err := stream.Send(&stat); err != nil {
			return err
		}
		i += 1

		time.Sleep(time.Second * 5)

	}

	// Should never get here
	return fmt.Errorf("Stats push ended unexpectedly")

}

// "localhost:4984" with port offset 2 -> "localhost:4986"
func ApplyPortOffset(mobileSvcHostPort string, portOffset int) (hostPortWithOffset string) {

	components := strings.Split(mobileSvcHostPort, ":")
	portStr := components[1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(fmt.Sprintf("Unable to apply port offset to %s", mobileSvcHostPort))
	}
	port += portOffset
	return fmt.Sprintf("%s:%d", components[0], port)

}

func StartGateway(bootstrapConfig GatewayBootstrapConfig) (*Gateway, error) {

	// Client setup
	gw := NewGateway(bootstrapConfig)
	err := gw.Start()
	return gw, err

}

func RunGatewayLegacyMode(pathToConfigFile string) {

	serverConfig, err := ReadServerConfig(SyncGatewayRunModeNormal, pathToConfigFile)
	if err != nil {
		base.Fatalf(base.KeyAll, "Error reading config file %s: %v", base.UD(pathToConfigFile), err)
	}

	// If the interfaces were not specified in either the config file or
	// on the command line, set them to the default values
	if serverConfig.Interface == nil {
		serverConfig.Interface = &DefaultInterface
	}
	if serverConfig.AdminInterface == nil {
		serverConfig.AdminInterface = &DefaultAdminInterface
	}

	_ = RunServer(serverConfig)

	select {} // block forever

}
