package sync_gateway

import (
	"context"
	"log"
	"time"

	"fmt"
	"strings"

	"encoding/json"

	"strconv"

	"github.com/couchbase/mobile-service/mobile_service"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"google.golang.org/grpc"
	"github.com/couchbaselabs/gocbconnstr"
	"github.com/couchbase/mobile-service"
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
	ServerContext *rest.ServerContext

	// The SG DB contexts, keyed by their path in the Config
	DbContexts map[string]*db.DatabaseContext

	// The "bootstrap config" for this gateway to be able to connect to Couchbase Server to get actual config
	BootstrapConfig GatewayBootstrapConfig
}

func NewGateway(bootstrapConfig GatewayBootstrapConfig) *Gateway {
	gw := Gateway{
		BootstrapConfig: bootstrapConfig,
		Uuid:            bootstrapConfig.Uuid,
		Hostname:        GATEWAY_HOSTNAME,
		Config:          map[string]*mobile_service.MetaKVPair{},
		DbContexts:      map[string]*db.DatabaseContext{},
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


func (gw *Gateway) LoadConfigSnapshot() {

	// Get snapshot of MetaKV tree under /mobile/
	keyMobileConfig := AddTrailingSlash(MOBILE_GATEWAY_CONFIG)
	metaKvPairs, err := gw.GrpcClient.MetaKVListAllChildren(context.Background(), &mobile_service.MetaKVPath{Path: keyMobileConfig})
	if err != nil {
		panic(fmt.Sprintf("Error getting metakv for mobile key %v.  Err: %v", keyMobileConfig, err))
	}
	for _, metakvPair := range metaKvPairs.Items {
		gw.Config[metakvPair.Path] = metakvPair
	}

}

func (gw *Gateway) RunServer() {

	// Parse json stored in metakv into a ServerConfig
	serverConfig, err := gw.LoadServerConfig()
	if err != nil {
		panic(fmt.Sprintf("Error loading server config.  Err: %v", err))

	}

	serverContext := rest.RunServer(serverConfig)

	gw.ServerContext = serverContext

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

		existingConfigKV, found := gw.Config[updatedConfigKV.Path]

		if strings.HasPrefix(updatedConfigKV.Path, MOBILE_CONFIG_DATABASES) {

			switch found {
			case true:
				// If we got here, we already know about this db key in the config state
				// Is the db being deleted or updated?
				switch updatedConfigKV.Rev {
				case "":
				case "null":
					// Db is being deleted
					if err := gw.HandleDbDelete(updatedConfigKV, existingConfigKV); err != nil {
						log.Printf("Error handling db delete: %v", err)
					}
				default:
					// Db is being updated
					if err := gw.HandleDbUpdate(updatedConfigKV, existingConfigKV); err != nil {
						log.Printf("Error handling db update: %v", err)
					}
				}
			case false:
				// We don't know about this db key, brand new
				if err := gw.HandleDbAdd(updatedConfigKV); err != nil {
					log.Printf("Error handling db add: %v", err)
				}
			}

		} else if strings.HasPrefix(updatedConfigKV.Path, MOBILE_GATEWAY_CONFIG) {
			// Server level config updated
			if err := gw.HandleServerConfigUpdated(updatedConfigKV, existingConfigKV); err != nil {
				log.Printf("Error handling server level config update: %v", err)
			}
		}

	}

}

func (gw *Gateway) HandleServerConfigUpdated(metaKvPair, existingDbConfig *mobile_service.MetaKVPair) error {

	gw.Config[metaKvPair.Path] = metaKvPair

	// TODO: reload server level config

	return nil

}

func (gw *Gateway) HandleDbDelete(incomingMetaKVPair, existingDbConfig *mobile_service.MetaKVPair) error {

	if len(incomingMetaKVPair.Value) != 0 {
		return fmt.Errorf("If db config is being deleted, incoming value should be empty")
	}

	// Delete from config map
	delete(gw.Config, incomingMetaKVPair.Path)

	// TODO: other db delete handling

	return nil
}

/*

-
*/

func (gw *Gateway) HandleDbAdd(metaKvPair *mobile_service.MetaKVPair) error {

	gw.Config[metaKvPair.Path] = metaKvPair

	dbConfig, err := gw.LoadDbConfig(metaKvPair.Path)
	if err != nil {
		return err
	}

	// The metakv pair path will be: /mobile/gateway/config/databases/database-1
	// Get the last item from the path and use that as the dbname
	lastItemPath, err := MetaKVLastItemPath(metaKvPair.Path)
	if err != nil {
		return err
	}
	dbConfig.Name = lastItemPath

	// Enhance the db config with the connection parameters from the gateway bootstrap config
	dbConfig.Server = &gw.BootstrapConfig.GoCBConnstr
	dbConfig.Username = gw.BootstrapConfig.CBUsername
	dbConfig.Password = gw.BootstrapConfig.CBPassword

	dbContext, err := gw.ServerContext.AddDatabaseFromConfig(dbConfig)
	if err != nil {
		return err
	}

	gw.DbContexts[metaKvPair.Path] = dbContext

	// TODO: other db add handling

	return nil
}

func (gw *Gateway) HandleDbUpdate(metaKvPair, existingDbConfig *mobile_service.MetaKVPair) error {

	// Find existing db context
	_, found := gw.DbContexts[metaKvPair.Path]
	if !found {
		return fmt.Errorf("Did not find existing db context for %v", metaKvPair.Path)
	}

	removed := gw.ServerContext.RemoveDatabase("test_data_bucket") // TODO: fix
	if !removed {
		return fmt.Errorf("Could not remove db: %v", metaKvPair.Path)

	}

	delete(gw.DbContexts, metaKvPair.Path)

	// Add a DB based on updated config
	return gw.HandleDbAdd(metaKvPair)

}

func (gw *Gateway) LoadServerConfig() (serverConfig *rest.ServerConfig, err error) {

	// Load the listener config
	listenerConfigMetaKV, found := gw.Config[MOBILE_GATEWAY_LISTENER_CONFIG]
	if !found {
		return nil, fmt.Errorf("Key not found: %v", MOBILE_GATEWAY_LISTENER_CONFIG)
	}

	serverListenerConfig := rest.ServerConfig{}

	if err := json.Unmarshal(listenerConfigMetaKV.Value, &serverListenerConfig); err != nil {
		return nil, err
	}

	// Load the general config
	generalConfigMetaKV, found := gw.Config[MOBILE_GATEWAY_GENERAL_CONFIG]
	if !found {
		return nil, fmt.Errorf("Key not found: %v", MOBILE_GATEWAY_GENERAL_CONFIG)
	}

	serverGeneralConfig := rest.ServerConfig{}

	if err := json.Unmarshal(generalConfigMetaKV.Value, &serverGeneralConfig); err != nil {
		return nil, err
	}

	// Copy over the values from listener into general
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

func (gw *Gateway) LoadDbConfig(path string) (dbConfig *rest.DbConfig, err error) {

	metaKvPair, found := gw.Config[path]
	if !found {
		return nil, fmt.Errorf("Key not found: %v", path)
	}

	dbConfigTemp := rest.DbConfig{}

	if err := json.Unmarshal(metaKvPair.Value, &dbConfigTemp); err != nil {
		return nil, err
	}

	return &dbConfigTemp, nil

}

func (gw *Gateway) PushStatsStreamWithReconnect() error {

	for {

		log.Printf("Starting push stats stream")

		if err := gw.PushStatsStream(); err != nil {
			log.Printf("Error pushing stats: %v", err)
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

func RunGateway(bootstrapConfig GatewayBootstrapConfig, pushStats bool) {

	// Client setup
	gw := NewGateway(bootstrapConfig)
	defer gw.Close()

	// Load snapshot of configuration from MetaKV
	gw.LoadConfigSnapshot()

	// Kick off goroutine to observe stream of metakv changes
	go func() {
		err := gw.ObserveMetaKVChangesRetry(AddTrailingSlash(MOBILE))
		if err != nil {
			log.Printf("Error observing metakv changes: %v", err)
		}
	}()

	// Kick off http/s server
	gw.RunServer()

	if pushStats {
		// Push stats (blocks)
		if err := gw.PushStatsStreamWithReconnect(); err != nil {
			panic(fmt.Sprintf("Error pushing stats: %v", err))
		}
	} else {
		select {}
	}

}

func RunGatewayLegacyMode(pathToConfigFile string) {

	serverConfig, err := rest.ReadServerConfig(rest.SyncGatewayRunModeNormal, pathToConfigFile)
	if err != nil {
		base.Fatalf(base.KeyAll, "Error reading config file %s: %v", base.UD(pathToConfigFile), err)
	}

	// If the interfaces were not specified in either the config file or
	// on the command line, set them to the default values
	if serverConfig.Interface == nil {
		serverConfig.Interface = &rest.DefaultInterface
	}
	if serverConfig.AdminInterface == nil {
		serverConfig.AdminInterface = &rest.DefaultAdminInterface
	}

	_ = rest.RunServer(serverConfig)

	select {} // block forever

}
