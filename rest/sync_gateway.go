package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/couchbase/mobile-service"
	msgrpc "github.com/couchbase/mobile-service/mobile_service_grpc"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/gocbconnstr"
	pkgerrors "github.com/pkg/errors"
	"google.golang.org/grpc"
)

// This is the "ServerContext" for a SyncGateway that receives it's configuration via the Mobile Service
// component running inside of Couchbase Server.
type SyncGateway struct {

	// The uuid of this sync gateway node (the same one used by CBGT and stored in a local file)
	Uuid string

	// The hostname of this sync gateway node, not sure where this would come from
	Hostname string

	// The GRPC client that is used to push stats and retrieve MetaKV config
	GrpcClient msgrpc.MobileServiceClient

	// The underlying client connection that is used by the GRPC client
	GrpcConn *grpc.ClientConn

	// A cached version of the MetaKV configuration state.
	// Key: path in metakv (eg, /mobile/config/databases/db1)
	// Val: a MetaKVPair whose value contains a JSON dictionary
	MetaKVCache map[string]*msgrpc.MetaKVPair

	// The SG server context
	// TODO: there are data races that need to be addressed when the metakv observer goroutine updates this concurrently w/
	//       other goroutines accessing this *ServerContext
	ServerContext *ServerContext

	// The "bootstrap config" for this gateway to be able to connect to Couchbase Server to get actual config
	BootstrapConfig BootstrapConfig

	// Context + cancel function for the CancelPushStats grpc stream
	PushStatsContext base.CancellableContext

	// Context + cancel function for the MetaKVObserveChildren grpc stream
	MetaKVObserveChildrenContext base.CancellableContext
}

// The bootstrap config might contain a list of Couchbase Server hostnames to connect to.
// This type is an enumeration of the different connection strategies, currently just
// to choose the first one in the list or to choose a random host in the list.  Before
// either strategy is employed, the list of bootstrap nodes is pre-filterd to only include
// nodes that are detected to be "active", meaning that it returns a 200 response on the
// ns-server /pools/default REST API.
type ChooseMobileSvcStrategy int

const (
	ChooseMobileSvcFirst ChooseMobileSvcStrategy = iota
	ChooseMobileSvcRandom
)

// Create a new Sync Gateway
func NewSyncGateway(bootstrapConfig BootstrapConfig) *SyncGateway {

	gw := SyncGateway{
		BootstrapConfig: bootstrapConfig,
		Uuid:            bootstrapConfig.Uuid,
		Hostname:        GATEWAY_HOSTNAME, // TODO: discover from the OS
		MetaKVCache:     map[string]*msgrpc.MetaKVPair{},
	}

	err := gw.ConnectMobileSvc(ChooseMobileSvcFirst)
	if err != nil {
		panic(fmt.Sprintf("Error connecting to mobile service: %v", err))
	}

	return &gw
}

// Start a Sync Gateway
func (gw *SyncGateway) Start() error {

	// Load snapshot of configuration from MetaKV
	if err := gw.LoadConfigSnapshot(); err != nil {
		return err
	}

	// Parse json stored in metakv into a ServerConfig
	serverConfig, err := gw.LoadServerConfig()
	if err != nil {
		return err
	}

	// Init logging
	// TODO: will anything before this call get logged?  Maybe we should bootstrap this with "basic logging" and have it overridden later once contacting the mobile service?
	InitLogging(serverConfig)

	// Kick off http listeners
	serverContext := StartHttpListeners(serverConfig, false)

	// Save the server context
	gw.ServerContext = serverContext

	// This has to be done separately _after_ the call to gw.ServerContext, because
	// without a valid gw.ServerContext, loading databases will fail.
	if err := gw.LoadDbConfigAllDbs(); err != nil {
		return err
	}

	// Kick off goroutine to observe stream of metakv changes.  To shut this goroutine down, call gw.Stop(),
	// which will call gw.CancelMetaKVObserveChildren()
	go func() {
		err := gw.ObserveMetaKVChangesRetry(mobile_service.KeyDirMobileRoot)
		if err != nil {
			base.Warnf(base.KeyMobileService, fmt.Sprintf("Error observing metakv changes: %v", err))

		}
	}()

	// Kick off goroutine to push stats.  To shut this goroutine down, call gw.Stop(),
	// which will call gw.CancelPushStats()
	go func() {
		if err := gw.PushStatsStreamWithReconnect(); err != nil {
			base.Warnf(base.KeyMobileService, fmt.Sprintf("Error pushing stats: %v.  Stats will no longer be pushed.", err))
		}
	}()

	return nil

}

// Synchronously load a snapshot of the mobile config from MetakV
func (gw *SyncGateway) LoadConfigSnapshot() error {

	// Get snapshot of MetaKV tree
	keyMobileConfig := mobile_service.KeyDirMobileGateway
	metaKvPairs, err := gw.GrpcClient.MetaKVListAllChildren(context.Background(), &msgrpc.MetaKVPath{Path: keyMobileConfig})
	if err != nil {
		return pkgerrors.Wrapf(err, "Error listing metakv children for path: %v", keyMobileConfig)
	}
	for _, metakvPair := range metaKvPairs.Items {
		gw.MetaKVCache[metakvPair.Path] = metakvPair
	}
	return nil
}

// Load the top-level configuration elements (General, Listener, ..)
func (gw *SyncGateway) LoadServerConfig() (serverConfig *ServerConfig, err error) {

	gotListenerConfig := false
	gotGeneralConfig := false
	serverListenerConfig := ServerConfig{}
	serverGeneralConfig := ServerConfig{}

	//Get the general + listener config
	for _, metaKvPair := range gw.MetaKVCache {
		switch {
		case metaKvPair.Path == mobile_service.KeyMobileGatewayListener:
			gotListenerConfig = true
			if err := json.Unmarshal(metaKvPair.Value, &serverListenerConfig); err != nil {
				return nil, pkgerrors.Wrapf(err, "Error unmarshalling")
			}

		case metaKvPair.Path == mobile_service.KeyMobileGatewayGeneral:
			gotGeneralConfig = true
			if err := json.Unmarshal(metaKvPair.Value, &serverGeneralConfig); err != nil {
				return nil, pkgerrors.Wrapf(err, "Error unmarshalling")
			}
		}

	}

	if !gotListenerConfig {
		return nil, fmt.Errorf("Did not find required MetaKV config: %v", mobile_service.KeyMobileGatewayListener)
	}

	if !gotGeneralConfig {
		return nil, fmt.Errorf("Did not find required MetaKV config: %v", mobile_service.KeyMobileGatewayGeneral)
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

// Load the the cached metakv configuration for all databases
func (gw *SyncGateway) LoadDbConfigAllDbs() error {

	// Second pass to get the databases
	for _, metaKvPair := range gw.MetaKVCache {
		switch {
		case strings.HasPrefix(metaKvPair.Path, mobile_service.KeyMobileGatewayDatabases):
			if err := gw.ProcessDatabaseMetaKVPair(metaKvPair); err != nil {
				return err
			}
		}

	}

	return nil

}

// Connect to the mobile service and:
//
//   - Push stats on a regular basis
//   - Receive configuration updates
func (gw *SyncGateway) ConnectMobileSvc(strategy ChooseMobileSvcStrategy) error {

	mobileSvcHostPort, err := gw.ChooseActiveMobileSvcGrpcEndpoint(strategy)
	if err != nil {
		return err
	}

	log.Printf("Connecting to mobile service: %v", mobileSvcHostPort) // Temp duplicate since logging not started yet

	base.Infof(base.KeyMobileService, "Connecting to mobile service: %v", mobileSvcHostPort)

	// Set up a connection to the server.
	conn, err := grpc.Dial(mobileSvcHostPort, grpc.WithInsecure())
	if err != nil {
		return err
	}

	// Save the grpc connection to be able to access it later if needed
	gw.GrpcConn = conn

	// Create client
	gw.GrpcClient = msgrpc.NewMobileServiceClient(conn)

	return nil

}

// Check the list of Couchbase Server hosts given in the bootstrap config, filter to
// remove nodes that are detected to be inactive (GET /pools/default != 200), and choose one based
// on the strategy parameter and return the host:port combination for the GRPC endpoint.
func (gw *SyncGateway) ChooseActiveMobileSvcGrpcEndpoint(strategy ChooseMobileSvcStrategy) (mobileSvcHostPort string, err error) {

	mobileServiceNodes, err := gw.ActiveMobileSvcGrpcEndpoints()
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

// Check the list of Couchbase Server hosts given in the bootstrap config, filter to
// remove nodes that are detected to be inactive (GET /pools/default != 200), and return the
// host:port combination for the GRPC endpoints of each of the nodes.
func (gw *SyncGateway) ActiveMobileSvcGrpcEndpoints() (mobileSvcHostPorts []string, err error) {

	// Get the raw connection spec from the bootstrap config
	connSpec, err := gocbconnstr.Parse(gw.BootstrapConfig.GoCBConnstr)
	if err != nil {
		return []string{}, err
	}

	// Filter out cb server addresses that don't give 200 responses to /pools/default,
	// which probably means they have been removed from the cluster.
	connSpec = base.FilterAddressesInCluster(connSpec, gw.BootstrapConfig.CBUsername, gw.BootstrapConfig.CBPassword)

	// Calculate the GRPC endpoint port based on the couchbase server port and the PortGrpcTlsOffset
	mobileSvcHostPorts = []string{}
	for _, address := range connSpec.Addresses {
		grpcTargetPort := mobile_service.PortGrpcTlsOffset + address.Port
		hostPort := fmt.Sprintf("%s:%d", address.Host, grpcTargetPort)
		mobileSvcHostPorts = append(mobileSvcHostPorts, hostPort)
	}

	return mobileSvcHostPorts, nil

}

// Observe changes in MetaKV in a retry loop.  Does _not_ try to re-establish a GRPC connection,
// since it assumes that something else in the system is handling that.
func (gw *SyncGateway) ObserveMetaKVChangesRetry(path string) error {
	for {
		base.Debugf(base.KeyMobileService, "Starting ObserveMetaKVChanges on path: %v", path)
		if err := gw.ObserveMetaKVChanges(path); err != nil {
			base.Infof(base.KeyMobileService, "ObserveMetaKVChanges on path: %v returned error: %+v.", path, err)

			// Has the context been cancelled?  If so, stop the retry loop
			isDone := false
			select {
			case <-gw.MetaKVObserveChildrenContext.Context.Done():
				isDone = true
			default:
				isDone = false
			}

			if isDone {
				base.Tracef(base.KeyMobileService, "ObserveMetaKVChanges for path %v returned error %v, but ObserveMetaKVChanges has been cancelled.  Not retrying", path, err)
				return nil
			}

			time.Sleep(time.Second * 1)
			gw.ObserveMetaKVChanges(path)
		}
	}
}

// Observe changes in MetaKV in a retry loop and delegates handling to other methods.  Does not retry on errors.
func (gw *SyncGateway) ObserveMetaKVChanges(path string) error {

	// Create and store a cancellable context for the ObserveMetaKVChnages grpc stream
	gw.MetaKVObserveChildrenContext = base.CancellableContext{}
	gw.MetaKVObserveChildrenContext.Context, gw.MetaKVObserveChildrenContext.CancelFunc = context.WithCancel(context.Background())

	// Create grpc stream
	stream, err := gw.GrpcClient.MetaKVObserveChildren(gw.MetaKVObserveChildrenContext.Context, &msgrpc.MetaKVPath{Path: path})
	if err != nil {
		return pkgerrors.Wrapf(err, "Error calling MetaKVObserveChildren for path: %v", path)
	}

	// Receive results from stream
	for {

		updatedConfigKV, err := stream.Recv()
		if err != nil {
			return pkgerrors.Wrapf(err, "Error calling stream.Recv() while observing MetaKV path: %v", path)
		}

		base.Tracef(base.KeyMobileService, "ObserveMetaKVChanges got metakv change for %v: %+v", path, updatedConfigKV)
		if strings.HasPrefix(updatedConfigKV.Path, mobile_service.KeyMobileGatewayDatabases) {
			if err := gw.ProcessDatabaseMetaKVPair(updatedConfigKV); err != nil {
				return err
			}
		} else if strings.HasPrefix(updatedConfigKV.Path, mobile_service.KeyMobileGatewayGeneral) {
			if err := gw.HandleGeneralConfigUpdated(updatedConfigKV); err != nil {
				return err
			}
		} else if strings.HasPrefix(updatedConfigKV.Path, mobile_service.KeyMobileGatewayListener) {
			if err := gw.HandleListenerConfigUpdated(updatedConfigKV); err != nil {
				return err
			}
		}

	}
}

// If a MetaKV entry related to database config is edited, handle the change by either
// deleting, updating, or adding a database to this Sync Gateway.
func (gw *SyncGateway) ProcessDatabaseMetaKVPair(metakvPair *msgrpc.MetaKVPair) error {
	existingMetaKVConfig, found := gw.MetaKVCache[metakvPair.Path]
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

func (gw *SyncGateway) HandleGeneralConfigUpdated(metaKvPair *msgrpc.MetaKVPair) error {

	gw.MetaKVCache[metaKvPair.Path] = metaKvPair

	// TODO: reload general config

	newServerConfig, err := gw.LoadServerConfig()
	if err != nil {
		return err
	}

	newContext := NewServerContextFromExisting(newServerConfig, gw.ServerContext)

	gw.ServerContext = newContext

	base.Warnf(base.KeyAll, "HandleGeneralConfigUpdated ignoring change: %v", metaKvPair.Path)

	return nil

}

func (gw *SyncGateway) HandleListenerConfigUpdated(metaKvPair *msgrpc.MetaKVPair) error {

	gw.MetaKVCache[metaKvPair.Path] = metaKvPair

	// TODO: reload listener config -- will require interrupting existing http listeners and creating new ones

	base.Warnf(base.KeyAll, "HandleListenerConfigUpdated ignoring change: %v", metaKvPair.Path)

	return nil

}

func (gw *SyncGateway) HandleDbDelete(metaKvPair, existingDbConfig *msgrpc.MetaKVPair) error {

	if len(metaKvPair.Value) != 0 {
		return fmt.Errorf("If db config is being deleted, incoming value should be empty")
	}

	// Delete from MetaKv config cache
	delete(gw.MetaKVCache, metaKvPair.Path)

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

func (gw *SyncGateway) HandleDbAdd(metaKvPair *msgrpc.MetaKVPair) error {

	gw.MetaKVCache[metaKvPair.Path] = metaKvPair

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

	return nil
}

func (gw *SyncGateway) HandleDbUpdate(metaKvPair, existingDbConfig *msgrpc.MetaKVPair) error {

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

func (gw *SyncGateway) LoadDbConfig(path string) (dbConfig *DbConfig, err error) {

	metaKvPair, found := gw.MetaKVCache[path]
	if !found {
		return nil, fmt.Errorf("Key not found: %v", path)
	}

	dbConfigTemp := DbConfig{}

	if err := json.Unmarshal(metaKvPair.Value, &dbConfigTemp); err != nil {
		return nil, pkgerrors.Wrapf(err, "Error unmarshalling db config")
	}

	return &dbConfigTemp, nil

}

func (gw *SyncGateway) PushStatsStreamWithReconnect() error {

	for {

		base.Debugf(base.KeyMobileService, "Starting push stats stream")

		if err := gw.PushStatsStream(); err != nil {
			base.Errorf(base.KeyMobileService, "Error pushing stats: %+v", err)
		}

		// TODO: this should be a backoff / retry and only give up after a number of retries
		// TODO: also, this should happen in a central place in the codebase rather than arbitrarily in the PushStatsStream()
		for {

			// Has the context been cancelled?  If so, stop the retry loop
			isDone := false
			select {
			case <-gw.PushStatsContext.Context.Done():
				isDone = true
			default:
				isDone = false
			}

			if isDone {
				base.Tracef(base.KeyMobileService, "PushStatsStream has been cancelled.  Not retrying")
				return nil
			}

			base.Debugf(base.KeyMobileService, "Attempting to reconnect to grpc server")

			err := gw.ConnectMobileSvc(ChooseMobileSvcRandom)
			if err != nil {

				base.Errorf(base.KeyMobileService, "Error connecting to grpc server: %+v.  Retrying", err)

				// Retry
				time.Sleep(time.Second)
				continue
			}

		}

	}

}

// TODO: this section needs complete overhaul, should be moved to it's own file/struct
func (gw *SyncGateway) PushStatsStream() error {

	// Create and store a cancellable context for the PushSTats grpc stream
	gw.PushStatsContext = base.CancellableContext{}
	gw.PushStatsContext.Context, gw.PushStatsContext.CancelFunc = context.WithCancel(context.Background())

	// Create grpc stream to push stats
	stream, err := gw.GrpcClient.SendStats(gw.PushStatsContext.Context)
	if err != nil {
		return pkgerrors.Wrapf(err, "Error opening stream to send stats")
	}
	gatewayMeta := msgrpc.Gateway{
		Uuid:     gw.Uuid,
		Hostname: gw.Hostname,
	}

	// The creds are needed as a temporary crutch so that the mobile-service can authenticate
	// the client before updating metakv
	// TODO: should be completely reworked.
	creds := msgrpc.Creds{
		Username: gw.BootstrapConfig.CBUsername,
		Password: gw.BootstrapConfig.CBPassword,
	}
	stats := []msgrpc.Stats{
		msgrpc.Stats{Gateway: &gatewayMeta, NumChangesFeeds: "10", Creds: &creds},
		msgrpc.Stats{Gateway: &gatewayMeta, NumChangesFeeds: "20", Creds: &creds},
		msgrpc.Stats{Gateway: &gatewayMeta, NumChangesFeeds: "30", Creds: &creds},
	}

	i := 0
	for {

		statIndex := i % len(stats)
		stat := stats[statIndex]
		base.Tracef(base.KeyMobileService, "Sending stat: %v", stat)
		if err := stream.Send(&stat); err != nil {
			return pkgerrors.Wrapf(err, "Error sending stat")
		}
		i += 1

		select {
		case <-time.After(time.Second * time.Duration(base.PushStatsFreqSeconds)):
		case <-gw.PushStatsContext.Context.Done():
			base.Debugf(base.KeyMobileService, "PushStatsContext cancelled, breaking out of PushStatsStream loop")
			break
		}

	}

	// Should never get here
	return fmt.Errorf("Stats push ended unexpectedly")

}

// Dummy method that just blocks forever
func (gw *SyncGateway) Wait() {
	select {}
}

// Close any resources associated with this Sync Gateway instance
func (gw *SyncGateway) Stop() error {

	gw.PushStatsContext.CancelFunc()
	gw.MetaKVObserveChildrenContext.CancelFunc()

	gw.GrpcConn.Close()
	gw.ServerContext.Close()

	return nil

}

// ---- Helper functions

// Starts and runs the server given its configuration.  This kicks off async goroutines
// and returns the ServerContext immediately.  If runPostStartup is true, it also kicks off
// any post-startup functionality associated with the ServerContext, which currently is to
// kick off replicators.
func StartHttpListeners(config *ServerConfig, runPostStartup bool) *ServerContext {

	PrettyPrint = config.Pretty

	base.Infof(base.KeyAll, "Console LogKeys: %v", base.ConsoleLogKey().EnabledLogKeys())
	base.Infof(base.KeyAll, "Console LogLevel: %v", base.ConsoleLogLevel())
	base.Infof(base.KeyAll, "Log Redaction Level: %s", config.Logging.RedactionLevel)

	if os.Getenv("GOMAXPROCS") == "" && runtime.GOMAXPROCS(0) == 1 {
		cpus := runtime.NumCPU()
		if cpus > 1 {
			runtime.GOMAXPROCS(cpus)
			base.Infof(base.KeyAll, "Configured Go to use all %d CPUs; setenv GOMAXPROCS to override this", cpus)
		}
	}

	SetMaxFileDescriptors(config.MaxFileDescriptors)

	// Set global bcrypt cost if configured
	if config.BcryptCost > 0 {
		if err := auth.SetBcryptCost(config.BcryptCost); err != nil {
			base.Fatalf(base.KeyAll, "Configuration error: %v", err)
		}
	}

	sc := NewServerContext(config)
	for _, dbConfig := range config.Databases {
		if _, err := sc.AddDatabaseFromConfig(dbConfig); err != nil {
			base.Fatalf(base.KeyAll, "Error opening database %s: %+v", base.MD(dbConfig.Name), err)
		}
	}

	if config.ProfileInterface != nil {
		//runtime.MemProfileRate = 10 * 1024
		base.Infof(base.KeyAll, "Starting profile server on %s", base.UD(*config.ProfileInterface))
		go func() {
			http.ListenAndServe(*config.ProfileInterface, nil)
		}()
	}

	if runPostStartup {
		go sc.PostStartup()
	}

	if *config.AdminInterface != base.RestTesterInterface {
		base.Infof(base.KeyAll, "Starting admin API server on %s", base.UD(*config.AdminInterface))
		go config.Serve(*config.AdminInterface, CreateAdminHandler(sc))
	} else {
		base.Infof(base.KeyAll, "RestTester mode: admin API server via in-memory operations")
	}

	if *config.Interface != base.RestTesterInterface {
		base.Infof(base.KeyAll, "Starting public API server on %s", base.UD(*config.Interface))
		go config.Serve(*config.Interface, CreatePublicHandler(sc))
	} else {
		base.Infof(base.KeyAll, "RestTester mode: public API server via in-memory operations")
	}

	return sc

}

// Start Sync Gateway with a bootstrap config to enable pulling the config from a Mobile Service node
func StartSyncGateway(bootstrapConfig BootstrapConfig) (*SyncGateway, error) {

	RegisterSignalHandler()
	defer panicHandler()()

	// Create and start Sync Gateway using bootstrap config
	gw := NewSyncGateway(bootstrapConfig)
	err := gw.Start()
	return gw, err

}

// Start Sync Gateway in legacy mode that expects a static file JSON config
func RunSyncGatewayLegacyMode(pathToConfigFile string) {

	RegisterSignalHandler()
	defer panicHandler()()

	InitLogging(config)

	// TODO: call ParseCommandLine() and extract subset of CLI params used in service scripts

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

	_ = StartHttpListeners(serverConfig, true)

	select {} // block forever

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

func InitLogging(serverConfig *ServerConfig) {

	if serverConfig == nil {
		base.Fatalf(base.KeyAll, "Error setting up logging due to nil serverConfig")
	}

	// Logging config will now have been loaded from command line
	// or from a sync_gateway config file so we can validate the
	// configuration and setup logging now
	warnings, err := serverConfig.setupAndValidateLogging()
	if err != nil {
		base.Fatalf(base.KeyAll, "Error setting up logging: %v", err)
	}

	// This is the earliest opportunity to log a startup indicator
	// that will be persisted in log files.
	base.LogSyncGatewayVersion()

	// Execute any deferred warnings from setup.
	for _, logFn := range warnings {
		logFn()
	}

}

func RegisterSignalHandler() {
	signalchannel := make(chan os.Signal, 1)
	signal.Notify(signalchannel, syscall.SIGHUP, os.Interrupt, os.Kill)

	go func() {
		for sig := range signalchannel {
			base.Infof(base.KeyAll, "Handling signal: %v", sig)
			switch sig {
			case syscall.SIGHUP:
				HandleSighup()
			case os.Interrupt, os.Kill:
				// Ensure log buffers are flushed before exiting.
				base.FlushLogBuffers()
				os.Exit(130) // 130 == exit code 128 + 2 (interrupt)
			}
		}
	}()
}

func panicHandler() (panicHandler func()) {
	return func() {
		// Recover from any panics to allow for graceful shutdown.
		if r := recover(); r != nil {
			base.Errorf(base.KeyAll, "Handling panic: %v", r)
			// Ensure log buffers are flushed before exiting.
			base.FlushLogBuffers()

			panic(r)
		}
	}

}
