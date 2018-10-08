package sync_gateway

import (
	"context"

	"github.com/couchbase/mobile-service/mobile_service"
	"strings"
	"fmt"
)

type MetaKVClient struct {
	gateway *Gateway
	context context.Context
}

func NewMetaKVClient() *MetaKVClient {

	// TODO: at some point, this will probably need a real bootstrap config
	// TODO: so that it can discover the mobile service port on the mobile service node
	bootstrapConfig := GatewayBootstrapConfig{}

	return &MetaKVClient{
		gateway: NewGateway(bootstrapConfig),
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

func (mkv *MetaKVClient) ListAllChildren(key string) (metakvPairs []*mobile_service.MetaKVPair, err error) {

	if !strings.HasSuffix(key, "/") {
		return nil, fmt.Errorf("Invalid key: %v does not end in a '/'", key)
	}

	metaKvPairs, err := mkv.gateway.GrpcClient.MetaKVListAllChildren(mkv.context, &mobile_service.MetaKVPath{Path: key})
	if err != nil {
		return nil, err
	}
	return metaKvPairs.Items, err

}

func (mkv *MetaKVClient) Get(key string) (value []byte, err error) {

	metaKvPair, err := mkv.gateway.GrpcClient.MetaKVGet(mkv.context, &mobile_service.MetaKVPath{Path: key})
	if err != nil {
		return nil, err
	}
	return metaKvPair.Value, nil

}

func (mkv *MetaKVClient) Delete(key string) (err error) {

	serverConfigKey, err  := mkv.gateway.GrpcClient.MetaKVGet(mkv.context, &mobile_service.MetaKVPath{
		Path: key,
	})

	if err != nil {
		return err
	}

	_, err = mkv.gateway.GrpcClient.MetaKVDelete(mkv.context, &mobile_service.MetaKVPair{
		Path: key,
		Rev: serverConfigKey.Rev,
	})

	return err

}

// /mobile/gateway/config/databases/database-1 -> database-1
func MetaKVLastItemPath(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("Cannot get last item from empty path")
	}
	components := strings.Split(path, "/")
	if len(components) == 0 {
		return "", fmt.Errorf("Not enough components found in path: %v", path)
	}
	return components[len(components) - 1], nil

}

