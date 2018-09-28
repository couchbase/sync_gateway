package main

import (
	"log"
	"fmt"
	"context"
	"github.com/couchbase/mobile-service/mobile_service"
	"github.com/couchbase/sync_gateway"
)

type AdminCLI struct {
	gateway *sync_gateway.Gateway
	context context.Context
}

func NewAdminCLI() *AdminCLI {
	return &AdminCLI{
		gateway: sync_gateway.NewGateway(),
		context: context.Background(),
	}
}

func (a *AdminCLI) UpsertServerConfig() {

	a.UpsertKey(sync_gateway.MOBILE_CONFIG_SERVER, "config-data/server.json")

}

func (a *AdminCLI) UpsertKey(key, assetPath string) {

	assetJson, err := Asset(assetPath)
	if err != nil {
		panic(fmt.Sprintf("Asset not found"))
	}

	log.Printf("assetJson: %s", string(assetJson))

	serverConfigKey, _  := a.gateway.GrpcClient.MetaKVGet(a.context, &mobile_service.MetaKVPath{
		Path: key,
	})

	if serverConfigKey.Rev == "" {
		_, err = a.gateway.GrpcClient.MetaKVAdd(a.context, &mobile_service.MetaKVPair{
			Path:  key,
			Value: assetJson,
		})

	} else {
		_, err = a.gateway.GrpcClient.MetaKVSet(a.context, &mobile_service.MetaKVPair{
			Path:  key,
			Rev: serverConfigKey.Rev,
			Value: assetJson,
		})
	}

	if err != nil {
		log.Printf("Error adding/setting key: %v. Err: %v", key, err)
	}
}


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

}