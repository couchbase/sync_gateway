package base

import (
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
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
)

//Structs used when checking health of nodes and buckets
type nodes struct {
	Status string `json:"status"`
	Uptime string `json:"uptime"`
}

type responseBody struct {
	Nodes []nodes `json:"nodes"`
}

//This will start up a Docker Instance of Couchbase Server and will be used to run tests against
func NewDockerTest(t *testing.M) {

	//Only spin up a Docker Instance if environment variable is set
	if os.Getenv("SG_TEST_USE_DOCKER") == "true" {
		err := os.Setenv("SG_TEST_BACKING_STORE", "Couchbase")

		//Delete any existing containers. This can occur if a test fails and is unable to shutdown its instance
		cli, err := docker.NewClient("")
		fatalError("Unable to create docker client", err)

		containers, err := cli.ListContainers(docker.ListContainersOptions{})
		fatalError("Unable to list existing containers", err)

		for _, container := range containers {
			if val, ok := container.Labels["purpose"]; ok && val == "sg_integration_tests" {
				fmt.Println(fmt.Sprintf("Old Couchbase Instance Found With ID %s and Image %s... Deleting", container.ID, container.Image))
				timeout := uint(10)
				err = cli.StopContainer(container.ID, timeout)
				if err != nil {
					fmt.Printf("Unable to stop existing container: %s", err.Error())
				}
				err = cli.RemoveContainer(docker.RemoveContainerOptions{ID: container.ID, Force: true})
				if err != nil {
					fmt.Printf("Unable to remove existing container: %s", err.Error())
				}
			}
		}

		var cluster *gocb.Cluster

		//Setup couchbase docker instance
		pool, err := dockertest.NewPool("")
		if err != nil {
			log.Fatalf("Could not connect to docker: %s", err)
		}

		resource, err := pool.RunWithOptions(&dockertest.RunOptions{Repository: "couchbase", Tag: "enterprise-6.0.1", Labels: map[string]string{"purpose": "sg_integration_tests"}}, func(config *docker.HostConfig) {
			config.PortBindings = map[docker.Port][]docker.PortBinding{

				//Assign all the ports necessary for Couchbase to communicate on
				"9119/tcp":  {{"0.0.0.0", "9119"}},
				"9998/tcp":  {{"0.0.0.0", "9998"}},
				"11213/tcp": {{"0.0.0.0", "11213"}},

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

		//Retry until Couchbase Server is online and responding
		if err = pool.Retry(func() error {
			var err error
			_, err = http.Get("http://127.0.0.1:8091/pools")
			if err != nil {
				return err
			}
			return nil

		}); err != nil {
			log.Fatalf("Could not connect to docker: %s", err)
		}

		//Setup hostname
		//xip.io hostname simply resolves to localhost. This allows us to to communicate with Couchbase on localhost
		//to prevent the issue where the internal docker IP is used. See xip.io website for more info.
		data := url.Values{}
		client := &http.Client{}
		data.Add("hostname", "127.0.0.1.xip.io")
		req, err := http.NewRequest("POST", "http://localhost:8091/node/controller/rename", strings.NewReader(data.Encode()))
		fatalError(`Unable to set hostname`, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetBasicAuth("Administrator", "password")
		_, err = client.Do(req)
		fatalError(`Unable to set hostname`, err)

		//Set index storage mode
		data = url.Values{}
		data.Set("storageMode", "plasma")
		req, err = http.NewRequest("POST", "http://localhost:8091/settings/indexes", strings.NewReader(data.Encode()))
		fatalError(`Unable to set index mode`, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetBasicAuth("Administrator", "password")
		_, err = client.Do(req)
		fatalError(`Unable to set index mode`, err)

		//Setup required services
		data = url.Values{}
		data.Set("services", "kv,n1ql,index,fts")
		response, err := http.PostForm("http://localhost:8091/node/controller/setupServices", data)
		fatalError(`Unable to setup services`, err)

		//Setup memory quotas for each service
		data = url.Values{}
		data.Set("memoryQuota", "3096")
		data.Set("indexMemoryQuota", "1024")
		data.Set("ftsMemoryQuota", "1024")
		response, err = http.PostForm("http://localhost:8091/pools/default", data)
		fatalError(`Unable to setup services`, err)

		//Set admin credentials
		data = url.Values{}
		data.Set("password", "password")
		data.Set("username", "Administrator")
		data.Set("port", "SAME")
		response, err = http.PostForm("http://localhost:8091/settings/web", data)
		fatalError(`Unable to set admin credentials`, err)

		//Define bucket specs
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

		//Define user specs
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

		//Connect to cluster with gocb and create users and buckets
		cluster, err = gocb.Connect("http://localhost:8091/")
		fatalError(`Unable to connect to cluster with gocb`, err)
		manager := cluster.Manager("Administrator", "password")

		err = manager.UpdateBucket(&bucketSettings)
		fatalError(`Unable to create bucket`, err)
		err = manager.UpdateBucket(&bucketSettings1)
		fatalError(`Unable to create bucket`, err)
		err = manager.UpdateBucket(&bucketSettings2)
		fatalError(`Unable to create bucket`, err)

		err = manager.UpsertUser("local", "test_data_bucket", &userSettings)
		fatalError(`Unable to create user`, err)
		err = manager.UpsertUser("local", "test_shadowbucket", &userSettings1)
		fatalError(`Unable to create user`, err)
		err = manager.UpsertUser("local", "test_indexbucket", &userSettings2)
		fatalError(`Unable to create user`, err)

		err = cluster.Close()
		if err != nil {
			fmt.Println("Failed to close gocb cluster connection but will continue")
		}

		//Poll buckets to ensure they are all healthy and ready to work
		req, err = http.NewRequest("GET", "http://localhost:8091/pools/default/buckets", nil)
		fatalError(`Failed to poll bucket status`, err)
		req.SetBasicAuth("Administrator", "password")
		response, err = client.Do(req)
		fatalError(`Failed to poll bucket status`, err)
		var jsonBody []responseBody
		err = json.NewDecoder(response.Body).Decode(&jsonBody)
		fatalError(`Failed to poll bucket status`, err)

		for !areBucketsReady(&jsonBody) {
			fmt.Println("Buckets Not Ready Yet Will Retry In 1 Second")
			time.Sleep(1 * time.Second)
			req, _ = http.NewRequest("GET", "http://localhost:8091/pools/default/buckets", nil)
			req.SetBasicAuth("Administrator", "password")
			response, err = client.Do(req)
			fatalError(`Failed to poll bucket status`, err)
			err = json.NewDecoder(response.Body).Decode(&jsonBody)
			fatalError(`Failed to poll bucket status`, err)
		}

		//Poll nodes / node to ensure they are all healthy and ready to work
		var jsonBody2 responseBody
		req, err = http.NewRequest("GET", "http://localhost:8091/pools/default", nil)
		fatalError(`Failed to poll node status`, err)
		req.SetBasicAuth("Administrator", "password")
		response, err = client.Do(req)
		fatalError(`Failed to poll node status`, err)
		err = json.NewDecoder(response.Body).Decode(&jsonBody2)

		for !isNodeReady(&jsonBody2) {
			fmt.Println("Node Not Ready Yet Will Retry In 1 Second")
			time.Sleep(1 * time.Second)
			req, err = http.NewRequest("GET", "http://localhost:8091/pools/default", nil)
			fatalError(`Failed to poll node status`, err)
			req.SetBasicAuth("Administrator", "password")
			response, err = client.Do(req)
			fatalError(`Failed to poll node status`, err)
			err = json.NewDecoder(response.Body).Decode(&jsonBody2)
			fatalError(`Failed to poll node status`, err)
		}

		//Run integration tests
		code := t.Run()

		//Remove Docker container when complete
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
	//TODO: Figure out a nice way of handling this other than waiting for 60 secs of uptime
	uptime, _ := strconv.Atoi(nres.Nodes[0].Uptime)
	if len(nres.Nodes) == 1 && nres.Nodes[0].Status != "healthy" || uptime <= 60 {
		ready = false
	}

	return ready
}

func fatalError(errorString string, err error) {
	if err != nil {
		fmt.Println(fmt.Sprintf("Fatal error occurred: %s Error: %s", errorString, err.Error()))
		os.Exit(1)
	}
}
