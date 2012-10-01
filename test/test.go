// test.go

package main

import "couchglue"
import "flag"
import "log"
import "net/http"
import "github.com/couchbaselabs/go-couchbase"


func main() {
	addr := flag.String("addr", ":4984", "Address to bind to")
	couchbaseURL := flag.String("url", "http://localhost:8091", "Address of Couchbase server")
	poolName := flag.String("pool", "default", "Name of pool")
	bucketName := flag.String("bucket", "couchdb", "Name of bucket")
	flag.Parse()
    
	bucket, err := couchglue.ConnectToBucket(*couchbaseURL, *poolName, *bucketName)
	if err != nil {
		log.Fatalf("Error getting bucket '%s':  %v\n", *bucketName, err)
	}
    
    couchglue.InitREST(bucket)
    
    log.Printf("Starting server on %s", *addr)
    err = http.ListenAndServe(*addr, nil)
    if err != nil {
        log.Fatal("Server failed: ", err.Error());
    }
}

/*
func main_client() {
	flag.Parse()

    url := flag.Arg(0)
    if url == "" { url = "http://localhost:8091" }
	c, err := couchbase.Connect(url)
	if err != nil {
		log.Fatalf("Error connecting to <%s>:  %v", url, err)
	}
	log.Printf("Connected to <%s>, ver=%s\n", url, c.Info.ImplementationVersion)
    
    poolName := "default"
	pool, err := c.GetPool(poolName)
	if err != nil {
		log.Fatalf("Can't get pool '%s':  %v", poolName, err)
	}

    bucketName := flag.Arg(1)
    if bucketName == "" { bucketName = "couchdb" }
	bucket, err := pool.GetBucket(bucketName)
	if err != nil {
		log.Fatalf("Error getting bucket '%s':  %v\n", bucketName, err)
	}
    
    db := couchglue.MakeDatabase(bucket, "db1")
    log.Printf("Created db: %v\n", db)
    
    value, err := db.Get("foo")
    log.Printf("Value = %v, error = %v", value, err)
    
    body := make(couchglue.Body)
    body["first"] = "Bob"
    body["last"] = "Dobbs"
    body["slack"] = 999999
    err = db.Put("foo", body)
	if err != nil {
		log.Fatalf("Error putting foo:  %v\n", err)
	}
    log.Printf("Successfully put foo.")
    
    body = make(couchglue.Body)
    body["first"] = "Oliver"
    body["last"] = "Boliver"
    body["slack"] = 0
    err = db.Put("bar", body)
	if err != nil {
		log.Fatalf("Error putting bar:  %v\n", err)
	}
    log.Printf("Successfully put bar.")
    
    docids, err := db.AllDocIDs()
	if err != nil {
		log.Fatalf("Error getting all doc IDs:  %v\n", err)
	}
    log.Printf("Doc IDs = %v", docids)
}
*/