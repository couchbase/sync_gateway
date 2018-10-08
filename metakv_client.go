package sync_gateway

import (
	"context"

	"github.com/couchbase/mobile-service/mobile_service"
)

type MetaKVClient struct {
	gateway *Gateway
	context context.Context
}

func NewMetaKVClient() *MetaKVClient {
	return &MetaKVClient{
		gateway: NewGateway(),
		context: context.Background(),
	}
}

func (mkv *MetaKVClient) Upsert(key string, value []byte) (err error) {

	// Get existing value at that key in order to get the metakv rev
	serverConfigKey, err := mkv.gateway.GrpcClient.MetaKVGet(mkv.context, &mobile_service.MetaKVPath{
		Path: key,
	})

	if err != nil {
		return err
	}

	if serverConfigKey.Rev == "" {
		// If no metakv rev, do an add
		_, err = mkv.gateway.GrpcClient.MetaKVAdd(mkv.context, &mobile_service.MetaKVPair{
			Path:  key,
			Value: value,
		})

	} else {
		// If there is a metakv rev, do a set
		_, err = mkv.gateway.GrpcClient.MetaKVSet(mkv.context, &mobile_service.MetaKVPair{
			Path:  key,
			Rev:   serverConfigKey.Rev,
			Value: value,
		})
	}

	return err

}

/*



// DeleteAllMobileKeys
func (a *AdminCLI) DeleteAllMobileKeys() {

	mobileConfigKey := sync_gateway.AddTrailingSlash(sync_gateway.MOBILE_CONFIG) // /mobile/config/
	_, err := a.gateway.GrpcClient.MetaKVRecursiveDelete(a.context, &mobile_service.MetaKVPath{Path: mobileConfigKey})
	if err != nil {
		panic(fmt.Sprintf("Error recursively deleting elements under %v.  Err: %v", mobileConfigKey, err))
	}

}

func (a *AdminCLI) ListAllMobileKeys() {

	mobileConfigKey := sync_gateway.AddTrailingSlash(sync_gateway.MOBILE_CONFIG) // /mobile/config/
	metaKvPairs, err := a.gateway.GrpcClient.MetaKVListAllChildren(a.context, &mobile_service.MetaKVPath{Path: mobileConfigKey})
	if err != nil {
		panic(fmt.Sprintf("Error getting metakv for mobile key %v.  Err: %v", mobileConfigKey, err))
	}
	for _, metakvPair := range metaKvPairs.Items {
		log.Printf("metakvPair: %+v", metakvPair)
	}

}

func (a *AdminCLI) UpsertDbConfig(dbName string) {

	dbKey := fmt.Sprintf("%s/%s", sync_gateway.MOBILE_CONFIG_DATABASES, dbName)

	a.UpsertKey(dbKey, fmt.Sprintf("config-data/%s.json", dbName))

}

func (a *AdminCLI) DeleteDbConfig(dbName string) {

	dbKey := fmt.Sprintf("%s/%s", sync_gateway.MOBILE_CONFIG_DATABASES, dbName)

	serverConfigKey, _  := a.gateway.GrpcClient.MetaKVGet(a.context, &mobile_service.MetaKVPath{
		Path: dbKey,
	})

	_, err := a.gateway.GrpcClient.MetaKVDelete(a.context, &mobile_service.MetaKVPair{
		Path: dbKey,
		Rev: serverConfigKey.Rev,
	})

	if err != nil {
		log.Printf("Error deleting key: %v. Err: %v", dbKey, err)
	}



 */