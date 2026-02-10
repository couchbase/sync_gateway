package rest

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

var tesDoc = `{"channels": ["%d"],
"testDoc": [
  {
    "_id": "666acdcd7bc4dbb3289a2b0a",
    "index": 0,
    "guid": "d8aca2f5-daac-47aa-8d8d-7f6692d5097e",
    "isActive": false,
    "balance": "$1,367.98",
    "picture": "http://placehold.it/32x32",
    "age": 26,
    "eyeColor": "blue",
    "name": "Aurora Wheeler",
    "gender": "female",
    "company": "BEZAL",
    "email": "aurorawheeler@bezal.com",
    "phone": "+1 (972) 570-2140",
    "address": "162 Newel Street, Kula, Georgia, 8484",
    "about": "Dolore nisi esse sit ullamco tempor do exercitation nisi mollit. Cupidatat incididunt consequat nostrud Lorem Lorem dolore irure. Veniam labore laborum et fugiat officia ad nostrud. Commodo quis qui cillum elit pariatur laborum duis veniam minim aliquip esse do et quis. Aliqua proident velit adipisicing laboris mollit qui enim Lorem ad commodo nostrud irure pariatur. Fugiat incididunt tempor id quis consequat tempor exercitation est eu officia cupidatat consectetur cupidatat cillum. Consectetur duis consequat cupidatat eu ex commodo consectetur duis reprehenderit sunt deserunt sint dolore qui."
  }
]}`

// will create users and issue chnages request in thier name
func (rt *RestTester) createUsers(num int, wg *sync.WaitGroup) {
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("user%d", num)
		rt.CreateUser(name, []string{fmt.Sprint(num)})
		resp := rt.SendUserRequest("GET", "/{{.keyspace}}/_changes", "", name)
		RequireStatus(rt.TB, resp, http.StatusOK)
		num++
	}
	wg.Done()
}

// add a doc to each channel
func (rt *RestTester) createDocs(num int, wg *sync.WaitGroup) {
	for i := 0; i < 1000; i++ {
		resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+RandomString(40), fmt.Sprintf(tesDoc, num))
		if resp.Code == http.StatusConflict {
			// try again
			resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+RandomString(60), fmt.Sprintf(tesDoc, num))
			if resp.Code == http.StatusConflict {
				continue
			}
			continue
		}
		rt.WaitForPendingChanges()
		num++
	}
	wg.Done()
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// generate random string for id
func RandomString(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func BenchmarkCaching(b *testing.B) {
	rt := NewRestTester(b, &RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				AutoImport: false,
				CacheConfig: &CacheConfig{
					ChannelCacheConfig: &ChannelCacheConfig{
						MaxNumber: base.IntPtr(300000),
					},
				},
			},
		},
	})
	defer rt.Close()

	count := 0

	_ = rt.GetSingleTestDatabaseCollectionWithUser()

	var startWg sync.WaitGroup
	var writeWg sync.WaitGroup

	for i := 0; i < 50; i++ {
		startWg.Add(1)
		go rt.createUsers(count, &startWg)
		count = count + 1000
	}

	startWg.Wait()
	count = 0
	for b.Loop() {
		count = 0
		for i := 0; i < 50; i++ {
			writeWg.Add(1)
			go rt.createDocs(count, &writeWg)
			count = count + 1000
		}
		writeWg.Wait()
		fmt.Println(rt.GetDatabase().DbStats.Cache().ChannelCacheRevsActive.Value())
	}
}
