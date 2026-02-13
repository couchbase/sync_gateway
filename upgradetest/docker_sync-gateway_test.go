package upgradetest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/goaux/decowriter"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

const (
	containerRepo = "couchbase/sync-gateway"
	// set an expiry on all test containers at startup to avoid indefinite leaking
	containerTTL = time.Minute * 30
)

type syncGatewayNode struct {
	tb        testing.TB
	bootstrap *bootstrapInfo
	tag       string
	resource  *dockertest.Resource
	readyOnce *sync.Once
}

func (sgn *syncGatewayNode) Close(ctx context.Context) {
	require.NoError(sgn.tb, sgn.resource.Expire(uint(1)))
	_ = sgn.resource.Close()
}

var bootstrapGroupID = uuid.NewString()

// configFilepathForTag returns a filepath with a config appropriate for the specified Sync Gateway version tag.
func configFilepathForTag(tb testing.TB, tag string, bootstrap *bootstrapInfo) string {
	if strings.HasPrefix(tag, "1.") || strings.HasPrefix(tag, "2.") {
		require.FailNowf(tb, "unsupported version", "upgradetest framework does not support versions older than 3.0 since it assumes persistent config")
	}

	adminAuth := "true"
	if base.UnitTestUrlIsWalrus() {
		adminAuth = "false"
	}

	path, err := filepath.Abs(filepath.Join(tb.TempDir(), "upgradetest-config.json"))
	require.NoError(tb, err)
	f, err := os.Create(path)
	require.NoError(tb, err)

	_, err = f.WriteString(`{
  "bootstrap": {
    "group_id": "` + bootstrapGroupID + `",
    "server": "` + bootstrap.server + `",
    "username": "` + bootstrap.username + `",
    "password": "` + bootstrap.password + `",
    "use_tls_server": false
  },
  "logging": {
    "console": {
      "log_level": "debug",
      "log_keys": [
        "*"
      ]
    }
  },
  "api": {
    "admin_interface": ":4985",
	"admin_interface_authentication": ` + adminAuth + `,
	"metrics_interface_authentication": ` + adminAuth + `
  },
  "unsupported": {
    "stats_log_frequency": "5s",
    "diagnostic_interface": ":4987"
  }
}`)
	require.NoError(tb, err)
	require.NoError(tb, f.Close())

	return path
}

func newSyncGatewayNode(ctx context.Context, tb testing.TB, tag string, bootstrap *bootstrapInfo) *syncGatewayNode {
	configPath := configFilepathForTag(tb, tag, bootstrap)
	r, err := globalDockertestPool.RunWithOptions(&dockertest.RunOptions{
		Repository:   containerRepo,
		Tag:          tag,
		Env:          []string{"SG_COLOR=" + os.Getenv("SG_COLOR")},
		Cmd:          []string{"/tmp/config.json"},
		ExposedPorts: []string{"4984/tcp", "4985/tcp", "4986/tcp", "4987/tcp"},
		Mounts:       []string{fmt.Sprintf("%s:%s", configPath, "/tmp/config.json")},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
	})
	require.NoError(tb, err)

	require.NoError(tb, r.Expire(uint(containerTTL.Seconds())))

	if testing.Verbose() {
		go func() {
			_ = globalDockertestPool.Client.Logs(docker.LogsOptions{
				Context:     ctx,
				Stderr:      true,
				Follow:      true,
				RawTerminal: false,
				Container:   r.Container.ID,
				ErrorStream: decowriter.New(tb.Output(), []byte(tag+" | "), nil),
			})
		}()
	}

	sgn := &syncGatewayNode{
		tb:        tb,
		tag:       tag,
		resource:  r,
		readyOnce: &sync.Once{},
		bootstrap: bootstrap,
	}

	return sgn
}

func (sgn *syncGatewayNode) waitForReady() {
	sgn.readyOnce.Do(func() {
		time.Sleep(time.Millisecond * 500) // brief initial wait before polling
		require.NoError(sgn.tb, globalDockertestPool.Retry(func() error {
			addr := "http://" + sgn.resource.GetHostPort("4985/tcp")
			resp, err := sgn.sendRequest(http.MethodGet, addr+"/_ping", nil)
			if err == nil && resp.StatusCode == http.StatusOK {
				return nil
			}

			sgn.tb.Logf("%s | Waiting for Admin REST API", sgn.tag)
			if err != nil {
				return err
			}

			return fmt.Errorf("%s | Admin REST API not ready yet: %v %v", sgn.tag, resp.StatusCode, resp.Status)
		}))
	})
}

func (sgn *syncGatewayNode) sendRequest(method, url string, reqBody []byte) (resp *http.Response, err error) {
	r, err := http.NewRequest(method, url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}
	resp, err = http.DefaultClient.Do(r)
	return resp, err
}

func (sgn *syncGatewayNode) sendAdminRequest(method, path string, reqBody []byte) (r *http.Response) {
	sgn.waitForReady()
	basicAuth := sgn.bootstrap.username + ":" + sgn.bootstrap.password + "@"
	url := "http://" + basicAuth + sgn.resource.GetHostPort("4985/tcp") + path
	resp, err := sgn.sendRequest(method, url, reqBody)
	require.NoError(sgn.tb, err)
	return resp
}

func (sgn *syncGatewayNode) sendPublicRequest(method, path string, reqBody []byte) (r *http.Response) {
	sgn.waitForReady()
	// TODO: public auth
	// basicAuth := sgn.bootstrap.username + ":" + sgn.bootstrap.password + "@"
	url := "http://" + sgn.resource.GetHostPort("4984/tcp") + path
	resp, err := sgn.sendRequest(method, url, reqBody)
	require.NoError(sgn.tb, err)
	return resp
}

type bootstrapInfo struct {
	server   string
	username string
	password string
}

func bootstrapInfoFromTestBucket(b *base.TestBucket) bootstrapInfo {
	un, pw, _ := b.BucketSpec.Auth.GetCredentials()
	// since we're running containers - localhost requests to Couchbase Server need routing back out to the host
	server := strings.ReplaceAll(b.BucketSpec.Server, "localhost", "host.docker.internal")
	server = strings.ReplaceAll(server, "127.0.0.1", "host.docker.internal")
	return bootstrapInfo{
		server:   server,
		username: un,
		password: pw,
	}
}

func requireOneOfStatusCode(tb testing.TB, resp *http.Response, expected ...int) {
	for _, code := range expected {
		if resp.StatusCode == code {
			return
		}
	}
	require.FailNowf(tb, "unexpected response status", "got %d %s but expected one of %v - %s", resp.StatusCode, resp.Status, expected, respBody(resp))
}

func startCRUD(ctx context.Context, tb testing.TB, sgn *syncGatewayNode) {
	const (
		numDocs = 1000
		opDelay = 1 * time.Millisecond
	)

	createDocs := func(ctx context.Context, tb testing.TB) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				i := rand.Intn(numDocs)
				resp := sgn.sendAdminRequest(http.MethodPut, fmt.Sprintf("/db/test-doc-%d", i), []byte(fmt.Sprintf(`{"test_doc": true,"value": %d}`, i)))
				requireOneOfStatusCode(tb, resp, http.StatusCreated, http.StatusConflict)
				time.Sleep(opDelay)
			}
		}
	}

	readDocs := func(ctx context.Context, tb testing.TB) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				i := rand.Intn(numDocs)
				resp := sgn.sendAdminRequest(http.MethodGet, fmt.Sprintf("/db/test-doc-%d", i), nil)
				requireOneOfStatusCode(tb, resp, http.StatusOK, http.StatusNotFound)
				time.Sleep(opDelay)
			}
		}
	}

	updateDocs := func(ctx context.Context, tb testing.TB) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				i := rand.Intn(numDocs)
				resp := sgn.sendAdminRequest(http.MethodGet, fmt.Sprintf("/db/test-doc-%d", i), nil)
				requireOneOfStatusCode(tb, resp, http.StatusOK, http.StatusNotFound)
				if resp.StatusCode == http.StatusOK {
					var revBody struct {
						Rev string `json:"_rev"`
					}
					require.NoError(tb, json.Unmarshal(respBody(resp), &revBody))
					resp := sgn.sendAdminRequest(http.MethodPut, fmt.Sprintf("/db/test-doc-%d?rev=%s", i, revBody.Rev), []byte(`{"test_doc": true,"updated": true}`))
					requireOneOfStatusCode(tb, resp, http.StatusCreated, http.StatusConflict)
				}
				time.Sleep(opDelay)
			}
		}
	}

	deleteDocs := func(ctx context.Context, tb testing.TB) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				i := rand.Intn(numDocs)
				resp := sgn.sendAdminRequest(http.MethodGet, fmt.Sprintf("/db/test-doc-%d", i), nil)
				if resp.StatusCode == http.StatusOK {
					var revBody struct {
						Rev string `json:"_rev"`
					}
					require.NoError(tb, json.Unmarshal(respBody(resp), &revBody))
					resp := sgn.sendAdminRequest(http.MethodDelete, fmt.Sprintf("/db/test-doc-%d?rev=%s", i, revBody.Rev), nil)
					requireOneOfStatusCode(tb, resp, http.StatusOK, http.StatusConflict)
				}
				time.Sleep(opDelay)
			}
		}
	}

	go createDocs(ctx, tb)
	go readDocs(ctx, tb)
	go updateDocs(ctx, tb)
	go deleteDocs(ctx, tb)
}

func respBody(r *http.Response) []byte {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		panic(fmt.Sprintf("Error reading response body: %v", err))
	}
	_ = r.Body.Close()
	return body
}
