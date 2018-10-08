package sync_gateway

import (
	"context"
	"log"
	"time"

	"fmt"
	"strings"

	"encoding/json"

	"github.com/couchbase/mobile-service/mobile_service"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
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
	ServerContext *rest.ServerContext

	// The SG DB contexts, keyed by their path in the Config
	DbContexts map[string]*db.DatabaseContext

	// The "bootstrap config" for this gateway to be able to connect to Couchbase Server to get actual config
	BootstrapConfig GatewayBootstrapConfig

}

func NewGateway(bootstrapConfig GatewayBootstrapConfig) *Gateway {
	gw := Gateway{
		BootstrapConfig: bootstrapConfig,
		Uuid:       GATEWAY_UUID,
		Hostname:   GATEWAY_HOSTNAME,
		Config:     map[string]*mobile_service.MetaKVPair{},
		DbContexts: map[string]*db.DatabaseContext{},
	}
	err := gw.ConnectMobileSvc(MOBILE_SERVICE_HOST_PORT)
	if err != nil {
		panic(fmt.Sprintf("Error connecting to mobile service: %v", err))
	}

	return &gw
}

// Connect to the mobile service and:
//
//   - Push stats on a regular basis
//   - Receive configuration updates
func (gw *Gateway) ConnectMobileSvc(mobileSvcAddr string) error {

	// Set up a connection to the server.
	conn, err := grpc.Dial(mobileSvcAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	gw.GrpcConn = conn

	// Create client
	gw.GrpcClient = mobile_service.NewMobileServiceClient(conn)

	return nil

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
	}
	if serverListenerConfig.AdminInterface != nil {
		serverGeneralConfig.AdminInterface = serverListenerConfig.AdminInterface
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
		Username: "Testuser",
		Password: "password",
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

func RunGateway(bootstrapConfig GatewayBootstrapConfig, pushStats bool) {

	// Client setup
	gw := NewGateway(bootstrapConfig)
	defer gw.Close()

	// Load snapshot of configuration from MetaKV
	gw.LoadConfigSnapshot()

	// Kick off goroutine to observe stream of metakv changes
	go func() {
		err := gw.ObserveMetaKVChanges(AddTrailingSlash(MOBILE))
		if err != nil {
			panic(fmt.Sprintf("Error observing metakv changes: %v", err))
		}
	}()

	// Kick off http/s server
	go gw.RunServer()

	if pushStats {
		// Push stats (blocks)
		if err := gw.PushStatsStream(); err != nil {
			panic(fmt.Sprintf("Error pushing stats: %v", err))
		}
	} else {
		select {}
	}

}
