package sgclient

import (
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"encoding/json"
	"io/ioutil"
)

type SgClient struct {
	hostPort string
	*http.Client
}

func NewSgClient(hostPort string) *SgClient {

	httpClient := &http.Client{}

	sgClient := &SgClient{
		hostPort: hostPort,
		Client:   httpClient,
	}
	return sgClient

}

func (sgc *SgClient) WaitApiAvailable() (err error) {

	rootEndpoint := sgc.EndpointUrl("/")

	retryWorker := func() (shouldRetry bool, err error, value interface{}) {
		resp, err := sgc.Get(rootEndpoint)
		if err != nil {
			return true, err, resp
		}
		defer resp.Body.Close()
		return false, nil, resp
	}

	err, _ = base.RetryLoop(
		"WaitApiAvailable",
		retryWorker,
		base.CreateMaxDoublingSleeperFunc(10, 25, 1000),
	)

	return err

}

func (sgc *SgClient) EndpointUrl(relativePath string) string {
	return fmt.Sprintf("http://%s%s", sgc.hostPort, relativePath)
}

func (sgc *SgClient) GetDb(dbName string) (*rest.DbConfig, error) {

	dbEndpoint := sgc.EndpointUrl(fmt.Sprintf("/%s", dbName))
	resp, err := sgc.Get(dbEndpoint)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("Db not found")
	}

	dbConfig := rest.DbConfig{}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(body, &dbConfig); err != nil {
		return nil, err
	}

	return &dbConfig, nil





}
