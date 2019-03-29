package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/gocb"
)

var cluster *gocb.Cluster

//var port8091 string

//func TestMain(t *testing.M) {
//	rest.NewDockerTest(t)
//
//}

type nodes struct {
	Status string `json:"status"`
}

type responseBody struct {
	Nodes []nodes `json:"nodes"`
}

func TestHello(t *testing.T) {
	client := &http.Client{}
	req, _ := http.NewRequest("GET", "http://localhost:8091/pools/default/buckets", nil)
	req.SetBasicAuth("Administrator", "password")
	response, _ := client.Do(req)
	var jsonBody []responseBody
	json.NewDecoder(response.Body).Decode(&jsonBody)

	for !areBucketsReady(&jsonBody) {
		fmt.Println("Buckets Not Ready Yet")
		req, _ = http.NewRequest("GET", "http://localhost:8091/pools/default/buckets", nil)
		req.SetBasicAuth("Administrator", "password")
		response, _ = client.Do(req)
		json.NewDecoder(response.Body).Decode(&jsonBody)
	}

	var jsonBody2 responseBody
	req, _ = http.NewRequest("GET", "http://localhost:8091/pools/default", nil)
	req.SetBasicAuth("Administrator", "password")
	response, _ = client.Do(req)
	json.NewDecoder(response.Body).Decode(&jsonBody2)

	for !isNodeReady(&jsonBody) {
		fmt.Println("Node Not Ready Yet")
		req, _ = http.NewRequest("GET", "http://localhost:8091/pools/default", nil)
		req.SetBasicAuth("Administrator", "password")
		response, _ = client.Do(req)
		json.NewDecoder(response.Body).Decode(&jsonBody2)
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

func isNodeReady(res *[]responseBody) bool {
	ready := true
	nres := *res
	for i := 0; i < len(nres); i++ {
		if len(nres[i].Nodes) == 1 && nres[i].Nodes[0].Status != "healthy" {
			ready = false
		}

		fmt.Println(nres[i].Nodes[0].Status)
	}

	return ready
}
