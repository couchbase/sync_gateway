package main

import (
	"github.com/couchbase/sync_gateway/base"
	"fmt"
	"github.com/couchbase/gocb"
	"log"
)

type AuthHandler struct {

}

func (a AuthHandler) GetCredentials() (username string, password string, bucketname string) {
	return "data-bucket", "password", "data-bucket"
}

func main() {
	key := "sg_user_doc_877"

	lookupInEx(key)

	// cas := lookupInEx(key)

	// deleteDoc(key, cas)

}

func deleteDoc(key string, cas gocb.Cas) {

	xattrKey := "_sync"
	xattrCasProperty := "_sync.cas"

	bucketSpec := base.BucketSpec{}
	bucketSpec.Server = "http://cbl.0:8091"
	bucketSpec.BucketName = "data-bucket"
	bucketSpec.Auth = AuthHandler{}

	bucket, err := base.GetCouchbaseBucketGoCB(bucketSpec)
	if err != nil {
		panic(fmt.Sprintf("Err: %v", err))
	}

	deleteBody := false
	mutateFlag := gocb.SubdocDocFlagAccessDeleted
	if deleteBody {
		mutateFlag = gocb.SubdocDocFlagNone
	}

	xv := map[string]interface{}{}
	builder := bucket.Bucket.MutateInEx(key, mutateFlag, cas, uint32(0)).
		UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                // Update the xattr
		UpsertEx(xattrCasProperty, "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the cas on the xattr
	if deleteBody {
		builder.RemoveEx("", gocb.SubdocFlagNone) // Delete the document body
	}
	docFragment, removeErr := builder.Execute()
	log.Printf("docFragment: %+v removeErr: %v", docFragment, removeErr)

}


func lookupInEx(key string) (casOut gocb.Cas) {
	xattrKey := "_sync"

	bucketSpec := base.BucketSpec{}
	bucketSpec.Server = "http://cbl.0:8091"
	bucketSpec.BucketName = "data-bucket"
	bucketSpec.Auth = AuthHandler{}

	bucket, err := base.GetCouchbaseBucketGoCB(bucketSpec)
	if err != nil {
		panic(fmt.Sprintf("Err: %v", err))
	}

	val := map[string]interface{}{}
	cas, err := bucket.Bucket.Get(key, &val)
	log.Printf("Get val: %v, cas: %v, err: %v", val, cas, err)


	res, lookupErr := bucket.Bucket.LookupInEx(key, gocb.SubdocDocFlagAccessDeleted).
		GetEx(xattrKey, gocb.SubdocFlagXattr).
		GetEx("", gocb.SubdocFlagNone).
		Execute()

	log.Printf("res: %+v, lookupErr: %v", res, lookupErr)

	xv := map[string]interface{}{}
	xattrContentErr := res.Content(xattrKey, &xv)
	if xattrContentErr != nil {
		panic(fmt.Sprintf("xattrContentErr: %v", xattrContentErr))
	}
	log.Printf("xv: %+v", xv)

	return res.Cas()
}


