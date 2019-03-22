package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/couchbase/gocb"
	"github.com/ory/dockertest"
)

var cluster *gocb.Cluster
var port8091 string

func TestMain(t *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.Run("couchbase", "enterprise-6.0.1", []string{})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	if err := pool.Retry(func() error {
		var err error
		port8091 = resource.GetPort("8091/tcp")
		cluster, err = gocb.Connect("http://localhost:" + port8091)
		if err != nil {
			return err
		}
		return nil

	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	time.Sleep(15 * time.Second)

	data := url.Values{}
	data.Set("services", "kv,n1ql,index,fts")

	//response, err := http.Post("http://localhost:"+port8091+"/node/controller/setupServices", "application/x-www-form-url-encoded", strings.NewReader(data.Encode()))
	response, err := http.PostForm("http://localhost:"+port8091+"/node/controller/setupServices", data)

	if err != nil {
		fmt.Println(err.Error())
	} else {
		bodyBytes, _ := ioutil.ReadAll(response.Body)
		fmt.Println(string(bodyBytes))
	}

	data = url.Values{}
	data.Set("memoryQuota", "512")
	data.Set("indexMemoryQuota", "256")

	response, err = http.PostForm("http://localhost:"+port8091+"/pools/default", data)

	if err != nil {
		fmt.Println(err.Error())
	} else {
		bodyBytes, _ := ioutil.ReadAll(response.Body)
		fmt.Println(string(bodyBytes))
	}

	data = url.Values{}
	data.Set("password", "password")
	data.Set("username", "Administrator")
	data.Set("port", "SAME")

	response, _ = http.PostForm("http://localhost:"+port8091+"/settings/web", data)

	if err != nil {
		fmt.Println(err.Error())
	} else {
		bodyBytes, _ := ioutil.ReadAll(response.Body)
		fmt.Println(string(bodyBytes))
	}

	bucketSettings := gocb.BucketSettings{
		FlushEnabled:  true,
		IndexReplicas: true,
		Name:          "test",
		Password:      "",
		Quota:         100,
		Replicas:      1,
		Type:          gocb.BucketType(0),
	}

	manager := cluster.Manager("Administrator", "password")
	err = manager.UpdateBucket(&bucketSettings)

	if err != nil {
		fmt.Println(err.Error())
	}

	t.Run()

	//if err := pool.Purge(resource); err != nil {
	//	log.Fatalf("Could not purge resource: %s", err)
	//}
}

func TestHello(t *testing.T) {

}
