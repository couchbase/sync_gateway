package rest

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

// Testing utilities that have been included in the rest package so that they
// are available to any package that imports rest.  (if they were in a _test.go
// file, they wouldn't be publicly exported to other packages)

var gBucketCounter = 0

type RestTester struct {
	_bucket          base.Bucket
	_sc              *ServerContext
	noAdminParty     bool         // Unless this is true, Admin Party is in full effect
	distributedIndex bool         // Test with walrus-based index bucket
	syncFn           string       // put the sync() function source in here (optional)
	cacheConfig      *CacheConfig // Cache options (optional)
}

func (rt *RestTester) Bucket() base.Bucket {
	if rt._bucket == nil {

		base.GetBucketOrPanic() // side effect of creating/flushing bucket
		spec := base.GetTestBucketSpec(base.DataBucket)

		username, password, _ := spec.Auth.GetCredentials()

		server := spec.Server
		gBucketCounter++

		var syncFnPtr *string
		if len(rt.syncFn) > 0 {
			syncFnPtr = &rt.syncFn
		}

		corsConfig := &CORSConfig{
			Origin:      []string{"http://example.com", "*", "http://staging.example.com"},
			LoginOrigin: []string{"http://example.com"},
			Headers:     []string{},
			MaxAge:      1728000,
		}

		rt._sc = NewServerContext(&ServerConfig{
			CORS:           corsConfig,
			Facebook:       &FacebookConfig{},
			AdminInterface: &DefaultAdminInterface,
		})

		_, err := rt._sc.AddDatabaseFromConfig(&DbConfig{
			BucketConfig: BucketConfig{
				Server:   &server,
				Bucket:   &spec.BucketName,
				Username: username,
				Password: password,
			},

			Name:        "db",
			Sync:        syncFnPtr,
			CacheConfig: rt.cacheConfig,
		})
		if err != nil {
			panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
		}
		rt._bucket = rt._sc.Database("db").Bucket

		if !rt.noAdminParty {
			rt.SetAdminParty(true)
		}

	}
	return rt._bucket
}

func (rt *RestTester) BucketAllowEmptyPassword() base.Bucket {

	//Create test DB with "AllowEmptyPassword" true
	server := "walrus:"
	bucketName := fmt.Sprintf("sync_gateway_test_%d", gBucketCounter)
	gBucketCounter++

	rt._sc = NewServerContext(&ServerConfig{
		CORS:           &CORSConfig{},
		Facebook:       &FacebookConfig{},
		AdminInterface: &DefaultAdminInterface,
	})

	_, err := rt._sc.AddDatabaseFromConfig(&DbConfig{
		BucketConfig: BucketConfig{
			Server: &server,
			Bucket: &bucketName},
		Name:               "db",
		AllowEmptyPassword: true,
		CacheConfig:        rt.cacheConfig,
	})

	if err != nil {
		panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
	}
	rt._bucket = rt._sc.Database("db").Bucket

	return rt._bucket
}

func (rt *RestTester) ServerContext() *ServerContext {
	rt.Bucket()
	return rt._sc
}

// Returns first database found for server context.
func (rt *RestTester) GetDatabase() *db.DatabaseContext {

	for _, database := range rt.ServerContext().AllDatabases() {
		return database
	}
	return nil
}

func (rt *RestTester) WaitForSequence(seq uint64) error {
	database := rt.GetDatabase()
	if database == nil {
		return fmt.Errorf("No database found")
	}
	return database.WaitForSequence(seq)
}

func (rt *RestTester) WaitForPendingChanges() error {
	database := rt.GetDatabase()
	if database == nil {
		return fmt.Errorf("No database found")
	}
	return database.WaitForPendingChanges()
}

func (rt *RestTester) SetAdminParty(partyTime bool) {
	a := rt.ServerContext().Database("db").Authenticator()
	guest, _ := a.GetUser("")
	guest.SetDisabled(!partyTime)
	var chans channels.TimedSet
	if partyTime {
		chans = channels.AtSequence(base.SetOf("*"), 1)
	}
	guest.SetExplicitChannels(chans)
	a.Save(guest)
}

func (rt *RestTester) Close() {
	if rt._sc != nil {
		rt._sc.Close()
	}
}

func (rt *RestTester) SendRequest(method, resource string, body string) *testResponse {
	return rt.Send(request(method, resource, body))
}

func (rt *RestTester) SendRequestWithHeaders(method, resource string, body string, headers map[string]string) *testResponse {
	req := request(method, resource, body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return rt.Send(req)
}

func (rt *RestTester) SendUserRequestWithHeaders(method, resource string, body string, headers map[string]string, username string, password string) *testResponse {
	req := request(method, resource, body)
	req.SetBasicAuth(username, password)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return rt.Send(req)
}
func (rt *RestTester) Send(request *http.Request) *testResponse {
	response := &testResponse{httptest.NewRecorder(), request}
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188
	CreatePublicHandler(rt.ServerContext()).ServeHTTP(response, request)
	return response
}

func (rt *RestTester) SendAdminRequest(method, resource string, body string) *testResponse {
	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	response := &testResponse{httptest.NewRecorder(), request}
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188

	CreateAdminHandler(rt.ServerContext()).ServeHTTP(response, request)
	return response
}

func (rt *RestTester) SendAdminRequestWithHeaders(method, resource string, body string, headers map[string]string) *testResponse {
	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	response := &testResponse{httptest.NewRecorder(), request}
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188

	CreateAdminHandler(rt.ServerContext()).ServeHTTP(response, request)
	return response
}

type testResponse struct {
	*httptest.ResponseRecorder
	rq *http.Request
}

func (r testResponse) DumpBody() {
	log.Printf("%v", string(r.Body.Bytes()))
}

func request(method, resource, body string) *http.Request {
	request, err := http.NewRequest(method, "http://localhost"+resource, bytes.NewBufferString(body))
	request.RequestURI = resource // This doesn't get filled in by NewRequest
	FixQuotedSlashes(request)
	if err != nil {
		panic(fmt.Sprintf("http.NewRequest failed: %v", err))
	}
	return request
}

func requestByUser(method, resource, body, username string) *http.Request {
	r := request(method, resource, body)
	r.SetBasicAuth(username, "letmein")
	return r
}

func assertStatus(t *testing.T, response *testResponse, expectedStatus int) {
	if response.Code != expectedStatus {
		t.Fatalf("Response status %d (expected %d) for %s <%s> : %s",
			response.Code, expectedStatus, response.rq.Method, response.rq.URL, response.Body)
	}
}
