package base

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocb"
	"github.com/docker/docker/api/types"
	client2 "github.com/docker/docker/client"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
	"github.com/pkg/errors"
)

type nodes struct {
	Status string `json:"status"`
	Uptime string `json:"uptime"`
}

type responseBody struct {
	Nodes []nodes `json:"nodes"`
}

func NewDockerTest(t *testing.M) {

	if os.Getenv("SG_TEST_USE_DOCKER") == "true" {
		os.Setenv("SG_TEST_BACKING_STORE", "Couchbase")

		cli, err := client2.NewClientWithOpts(client2.WithVersion("1.39"))
		if err != nil {
			panic(err)
		}

		containers, err := cli.ContainerList(context.Background(), types.ContainerListOptions{})
		if err != nil {
			panic(errors.New("Err Ma Gawd"))
		}

		for _, container := range containers {
			fmt.Printf("%s %s\n", container.ID[:10], container.Image)
			timeout := time.Duration(10) * time.Second
			err = cli.ContainerStop(context.Background(), container.ID, &timeout)
			err = cli.ContainerRemove(context.Background(), container.ID, types.ContainerRemoveOptions{})
		}

		var cluster *gocb.Cluster

		pool, err := dockertest.NewPool("")
		if err != nil {
			log.Fatalf("Could not connect to docker: %s", err)
		}

		resource, err := pool.RunWithOptions(&dockertest.RunOptions{Repository: "couchbase", Tag: "enterprise-6.0.1"}, func(config *docker.HostConfig) {
			config.PortBindings = map[docker.Port][]docker.PortBinding{
				"8091/tcp": {{"0.0.0.0", "8091"}},
				"8092/tcp": {{"0.0.0.0", "8092"}},
				"8093/tcp": {{"0.0.0.0", "8093"}},
				"8094/tcp": {{"0.0.0.0", "8094"}},
				"8095/tcp": {{"0.0.0.0", "8095"}},
				"8096/tcp": {{"0.0.0.0", "8096"}},

				"11207/tcp": {{"0.0.0.0", "11207"}},
				"11210/tcp": {{"0.0.0.0", "11210"}},
				"11211/tcp": {{"0.0.0.0", "11211"}},

				"18091/tcp": {{"0.0.0.0", "18091"}},
				"18092/tcp": {{"0.0.0.0", "18092"}},
				"18093/tcp": {{"0.0.0.0", "18093"}},
				"18094/tcp": {{"0.0.0.0", "18094"}},
				"18095/tcp": {{"0.0.0.0", "18095"}},
				"18096/tcp": {{"0.0.0.0", "18096"}},
			}
		})
		if err != nil {
			log.Fatalf("Could not start resource: %s", err)
		}

		if err = pool.Retry(func() error {
			var err error
			_, err = http.Get("http://localhost:8091/pools")
			if err != nil {
				return err
			}
			return nil

		}); err != nil {
			log.Fatalf("Could not connect to docker: %s", err)
		}

		cluster, err = gocb.Connect("http://localhost:8091/")

		data := url.Values{}
		client := &http.Client{}
		data.Set("storageMode", "plasma")

		req, err := http.NewRequest("POST", "http://localhost:8091/settings/indexes", strings.NewReader(data.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetBasicAuth("Administrator", "password")
		client.Do(req)

		data = url.Values{}
		data.Set("services", "kv,n1ql,index,fts")

		response, err := http.PostForm("http://localhost:8091/node/controller/setupServices", data)

		if err != nil {
			panic(errors.New("Unable to setup services"))
		}

		data = url.Values{}
		data.Set("memoryQuota", "3096")
		data.Set("indexMemoryQuota", "1024")
		data.Set("ftsMemoryQuota", "1024")

		response, err = http.PostForm("http://localhost:8091/pools/default", data)

		if err != nil {
			panic(errors.New("Unable to setup quota"))
		}

		data = url.Values{}
		data.Set("password", "password")
		data.Set("username", "Administrator")
		data.Set("port", "SAME")

		response, _ = http.PostForm("http://localhost:8091/settings/web", data)

		if err != nil {
			panic(errors.New("Unable to setup credentials"))
		}

		bucketSettings := gocb.BucketSettings{
			FlushEnabled:  true,
			IndexReplicas: false,
			Name:          "test_data_bucket",
			Password:      "",
			Quota:         1024,
			Replicas:      0,
			Type:          gocb.BucketType(0),
		}

		bucketSettings1 := gocb.BucketSettings{
			FlushEnabled:  true,
			IndexReplicas: false,
			Name:          "test_shadowbucket",
			Password:      "",
			Quota:         1024,
			Replicas:      0,
			Type:          gocb.BucketType(0),
		}

		bucketSettings2 := gocb.BucketSettings{
			FlushEnabled:  true,
			IndexReplicas: false,
			Name:          "test_indexbucket",
			Password:      "",
			Quota:         1024,
			Replicas:      0,
			Type:          gocb.BucketType(0),
		}

		userSettings := gocb.UserSettings{
			Password: "password",
			Roles: []gocb.UserRole{
				{"bucket_full_access", "test_data_bucket"},
				{"bucket_admin", "test_data_bucket"},
			},
		}

		userSettings1 := gocb.UserSettings{
			Password: "password",
			Roles: []gocb.UserRole{
				{"bucket_full_access", "test_shadowbucket"},
				{"bucket_admin", "test_data_bucket"},
			},
		}

		userSettings2 := gocb.UserSettings{
			Password: "password",
			Roles: []gocb.UserRole{
				{"bucket_full_access", "test_indexbucket"},
				{"bucket_admin", "test_indexbucket"},
			},
		}

		manager := cluster.Manager("Administrator", "password")
		err = manager.UpdateBucket(&bucketSettings)
		err = manager.UpdateBucket(&bucketSettings1)
		err = manager.UpdateBucket(&bucketSettings2)

		err = manager.UpsertUser("local", "test_data_bucket", &userSettings)
		err = manager.UpsertUser("local", "test_shadowbucket", &userSettings1)
		err = manager.UpsertUser("local", "test_indexbucket", &userSettings2)

		req, err = http.NewRequest("GET", "http://localhost:8091/pools/default/buckets", nil)
		req.SetBasicAuth("Administrator", "password")
		response, err = client.Do(req)
		var jsonBody []responseBody
		json.NewDecoder(response.Body).Decode(&jsonBody)

		for !areBucketsReady(&jsonBody) {
			fmt.Println("Buckets Not Ready Yet")
			req, _ = http.NewRequest("GET", "http://localhost:8091/pools/default/buckets", nil)
			req.SetBasicAuth("Administrator", "password")
			response, err = client.Do(req)
			json.NewDecoder(response.Body).Decode(&jsonBody)
		}

		var jsonBody2 responseBody
		req, err = http.NewRequest("GET", "http://localhost:8091/pools/default", nil)
		req.SetBasicAuth("Administrator", "password")
		response, err = client.Do(req)
		json.NewDecoder(response.Body).Decode(&jsonBody2)

		for !isNodeReady(&jsonBody2) {
			fmt.Println("Node Not Ready Yet")
			req, _ = http.NewRequest("GET", "http://localhost:8091/pools/default", nil)
			req.SetBasicAuth("Administrator", "password")
			response, err = client.Do(req)
			json.NewDecoder(response.Body).Decode(&jsonBody2)
		}

		if err != nil {
			fmt.Println(err.Error())
		}

		code := t.Run()

		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}

		os.Exit(code)
	} else {
		t.Run()
	}
}

func areBucketsReady(res *[]responseBody) bool {
	allReady := true
	nres := *res
	for i := 0; i < len(nres); i++ {
		if len(nres[i].Nodes) == 1 && nres[i].Nodes[0].Status != "healthy" {
			allReady = false
		}
	}
	return allReady
}

func isNodeReady(res *responseBody) bool {
	ready := true
	nres := *res
	uptime, _ := strconv.Atoi(nres.Nodes[0].Uptime)
	if len(nres.Nodes) == 1 && nres.Nodes[0].Status != "healthy" || uptime <= 20 {
		ready = false
	}
	fmt.Println(nres.Nodes)

	return ready
}

func clearExistingCouchbaseContainers() {
	cli, err := client2.NewClientWithOpts(client2.WithVersion("1.39"))
	if err != nil {
		panic(err)
	}

	containers, err := cli.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		fmt.Println(err.Error())
	}

	for _, container := range containers {
		fmt.Printf("%s %s\n", container.ID[:10], container.Image)
	}
}
