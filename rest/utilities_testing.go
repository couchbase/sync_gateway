package rest

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/go-blip"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
)

// Testing utilities that have been included in the rest package so that they
// are available to any package that imports rest.  (if they were in a _test.go
// file, they wouldn't be publicly exported to other packages)

var gBucketCounter = 0

type RestTesterConfig struct {
	noAdminParty          bool      // Unless this is true, Admin Party is in full effect
	SyncFn                string    // put the sync() function source in here (optional)
	DatabaseConfig        *DbConfig // Supports additional config options.  BucketConfig, Name, Sync, Unsupported will be ignored (overridden)
	NoFlush               bool      // Skip bucket flush step during creation.  Used by tests that need to simulate start/stop of Sync Gateway with backing bucket intact.
	InitSyncSeq           uint64    // If specified, initializes _sync:seq on bucket creation.  Not supported when running against walrus
	EnableNoConflictsMode bool      // Enable no-conflicts mode.  By default, conflicts will be allowed, which is the default behavior
	distributedIndex      bool      // Test with walrus-based index bucket
}

type RestTester struct {
	*RestTesterConfig
	tb                      testing.TB
	RestTesterBucket        base.Bucket
	RestTesterServerContext *ServerContext
	AdminHandler            http.Handler
	adminHandlerOnce        sync.Once
	PublicHandler           http.Handler
	publicHandlerOnce       sync.Once
}

func NewRestTester(tb testing.TB, restConfig *RestTesterConfig) *RestTester {
	var rt RestTester
	if tb == nil {
		panic("tester parameter cannot be nil")
	}
	rt.tb = tb
	if restConfig != nil {
		rt.RestTesterConfig = restConfig
	} else {
		rt.RestTesterConfig = &RestTesterConfig{}
	}
	return &rt
}

func NewRestTesterWithBucket(tb testing.TB, restConfig *RestTesterConfig, bucket base.Bucket) *RestTester {
	rt := NewRestTester(tb, restConfig)
	if bucket == nil {
		panic("nil bucket supplied. Use NewRestTester if you aren't supplying a bucket")
	}
	rt.RestTesterBucket = bucket

	return rt
}

func (rt *RestTester) Bucket() base.Bucket {

	if rt.tb == nil {
		panic("RestTester not properly initialized please use NewRestTester function")
	}

	if rt.RestTesterBucket != nil {
		return rt.RestTesterBucket
	}

	// Put this in a loop in case certain operations fail, like waiting for GSI indexes to be empty.
	// Limit number of attempts to 2.
	for i := 0; i < 2; i++ {

		// Initialize the bucket.  For couchbase-backed tests, triggers with creation/flushing of the bucket
		if !rt.NoFlush {
			tempBucket := base.GetTestBucket(rt.tb) // side effect of creating/flushing bucket
			if rt.InitSyncSeq > 0 {
				log.Printf("Initializing %s to %d", base.SyncSeqKey, rt.InitSyncSeq)
				_, incrErr := tempBucket.Incr(base.SyncSeqKey, rt.InitSyncSeq, rt.InitSyncSeq, 0)
				if incrErr != nil {
					rt.tb.Fatalf("Error initializing %s in test bucket: %v", base.SyncSeqKey, incrErr)
					return nil
				}
			}
			tempBucket.Close()
		} else {
			if rt.InitSyncSeq > 0 {
				rt.tb.Fatal("RestTester doesn't support NoFlush and InitSyncSeq in same test")
				return nil
			}
		}

		spec := base.GetTestBucketSpec(base.DataBucket)

		username, password, _ := spec.Auth.GetCredentials()

		server := spec.Server
		gBucketCounter++

		var syncFnPtr *string
		if len(rt.SyncFn) > 0 {
			syncFnPtr = &rt.SyncFn
		}

		corsConfig := &CORSConfig{
			Origin:      []string{"http://example.com", "*", "http://staging.example.com"},
			LoginOrigin: []string{"http://example.com"},
			Headers:     []string{},
			MaxAge:      1728000,
		}

		rt.RestTesterServerContext = NewServerContext(&ServerConfig{
			CORS:           corsConfig,
			Facebook:       &FacebookConfig{},
			AdminInterface: &DefaultAdminInterface,
		})

		useXattrs := base.TestUseXattrs()

		if rt.DatabaseConfig == nil {
			// If no db config was passed in, create one
			rt.DatabaseConfig = &DbConfig{}
		}

		// Force views if running against walrus
		if !base.TestUseCouchbaseServer() {
			rt.DatabaseConfig.UseViews = true
		}

		// numReplicas set to 0 for test buckets, since it should assume that there may only be one indexing node.
		numReplicas := uint(0)
		rt.DatabaseConfig.NumIndexReplicas = &numReplicas

		rt.DatabaseConfig.BucketConfig = BucketConfig{
			Server:   &server,
			Bucket:   &spec.BucketName,
			Username: username,
			Password: password,
		}
		rt.DatabaseConfig.Name = "db"
		rt.DatabaseConfig.Sync = syncFnPtr
		rt.DatabaseConfig.EnableXattrs = &useXattrs
		if rt.EnableNoConflictsMode {
			boolVal := false
			rt.DatabaseConfig.AllowConflicts = &boolVal
		}

		_, err := rt.RestTesterServerContext.AddDatabaseFromConfig(rt.DatabaseConfig)
		if err != nil {
			rt.tb.Fatalf("Error from AddDatabaseFromConfig: %v", err)
			return nil
		}
		rt.RestTesterBucket = rt.RestTesterServerContext.Database("db").Bucket

		// As long as bucket flushing wasn't disabled, wait for index to be empty (if this is a gocb bucket)
		if !rt.NoFlush {
			asGoCbBucket, isGoCbBucket := base.AsGoCBBucket(rt.RestTesterBucket)
			if isGoCbBucket {
				if err := db.WaitForIndexEmpty(asGoCbBucket, spec.UseXattrs); err != nil {
					base.Infof(base.KeyAll, "WaitForIndexEmpty returned an error: %v.  Dropping indexes and retrying", err)
					// if WaitForIndexEmpty returns error, drop the indexes and retry
					if err := base.DropAllBucketIndexes(asGoCbBucket); err != nil {
						rt.tb.Fatalf("Failed to drop bucket indexes: %v", err)
						return nil
					}

					continue // Go to the top of the for loop to retry
				}
			}
		}

		if !rt.noAdminParty {
			rt.SetAdminParty(true)
		}

		return rt.RestTesterBucket
	}

	rt.tb.Fatalf("Failed to create a RestTesterBucket after multiple attempts")
	return nil
}

func (rt *RestTester) BucketAllowEmptyPassword() base.Bucket {

	//Create test DB with "AllowEmptyPassword" true
	server := "walrus:"
	bucketName := fmt.Sprintf("sync_gateway_test_%d", gBucketCounter)
	gBucketCounter++

	rt.RestTesterServerContext = NewServerContext(&ServerConfig{
		CORS:           &CORSConfig{},
		Facebook:       &FacebookConfig{},
		AdminInterface: &DefaultAdminInterface,
	})

	_, err := rt.RestTesterServerContext.AddDatabaseFromConfig(&DbConfig{
		BucketConfig: BucketConfig{
			Server: &server,
			Bucket: &bucketName},
		Name:               "db",
		AllowEmptyPassword: true,
		UseViews:           true, // walrus only supports views
	})

	if err != nil {
		rt.tb.Fatalf("Error from AddDatabaseFromConfig: %v", err)
	}
	rt.RestTesterBucket = rt.RestTesterServerContext.Database("db").Bucket

	return rt.RestTesterBucket
}

func (rt *RestTester) ServerContext() *ServerContext {
	rt.Bucket()
	return rt.RestTesterServerContext
}

// Returns first database found for server context.
func (rt *RestTester) GetDatabase() *db.DatabaseContext {

	for _, database := range rt.ServerContext().AllDatabases() {
		return database
	}
	return nil
}

func (rt *RestTester) MustWaitForDoc(docid string, t testing.TB) {
	err := rt.WaitForDoc(docid)
	goassert.True(t, err == nil)

}

func (rt *RestTester) WaitForDoc(docid string) (err error) {
	seq, err := rt.SequenceForDoc(docid)
	if err != nil {
		return err
	}
	return rt.WaitForSequence(seq)
}

func (rt *RestTester) SequenceForDoc(docid string) (seq uint64, err error) {
	database := rt.GetDatabase()
	if database == nil {
		return 0, fmt.Errorf("No database found")
	}
	doc, err := database.GetDocument(docid, db.DocUnmarshalAll)
	if err != nil {
		return 0, err
	}
	return doc.Sequence, nil
}

// Wait for sequence to be buffered by the channel cache
func (rt *RestTester) WaitForSequence(seq uint64) error {
	database := rt.GetDatabase()
	if database == nil {
		return fmt.Errorf("No database found")
	}
	return database.WaitForSequence(context.TODO(), seq)
}

func (rt *RestTester) WaitForPendingChanges() error {
	database := rt.GetDatabase()
	if database == nil {
		return fmt.Errorf("No database found")
	}
	return database.WaitForPendingChanges(context.TODO())
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

func (rt *RestTester) DisableGuestUser() {
	rt.SetAdminParty(false)
}

func (rt *RestTester) Close() {
	if rt.tb == nil {
		panic("RestTester not properly initialized please use NewRestTester function")
	}
	if rt.RestTesterServerContext != nil {
		rt.RestTesterServerContext.Close()
	}
}

func (rt *RestTester) SendRequest(method, resource string, body string) *TestResponse {
	return rt.Send(request(method, resource, body))
}

func (rt *RestTester) SendRequestWithHeaders(method, resource string, body string, headers map[string]string) *TestResponse {
	req := request(method, resource, body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return rt.Send(req)
}

func (rt *RestTester) SendUserRequestWithHeaders(method, resource string, body string, headers map[string]string, username string, password string) *TestResponse {
	req := request(method, resource, body)
	req.SetBasicAuth(username, password)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return rt.Send(req)
}
func (rt *RestTester) Send(request *http.Request) *TestResponse {
	response := &TestResponse{httptest.NewRecorder(), request}
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188
	rt.TestPublicHandler().ServeHTTP(response, request)
	return response
}

func (rt *RestTester) TestAdminHandlerNoConflictsMode() http.Handler {
	rt.EnableNoConflictsMode = true
	return rt.TestAdminHandler()
}

func (rt *RestTester) TestAdminHandler() http.Handler {
	rt.adminHandlerOnce.Do(func() {
		rt.AdminHandler = CreateAdminHandler(rt.ServerContext())
	})
	return rt.AdminHandler
}

func (rt *RestTester) TestPublicHandler() http.Handler {
	rt.publicHandlerOnce.Do(func() {
		rt.PublicHandler = CreatePublicHandler(rt.ServerContext())
	})
	return rt.PublicHandler
}

type changesResults struct {
	Results  []db.ChangeEntry
	Last_Seq interface{}
}

func (rt *RestTester) CreateWaitForChangesRetryWorker(numChangesExpected int, changesUrl, username string, useAdminPort bool) (worker base.RetryWorker) {

	waitForChangesWorker := func() (shouldRetry bool, err error, value interface{}) {

		var changes changesResults
		var response *TestResponse

		if useAdminPort {
			response = rt.SendAdminRequest("GET", changesUrl, "")

		} else {
			response = rt.Send(requestByUser("GET", changesUrl, "", username))
		}
		err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
		if err != nil {
			return false, err, nil
		}
		if len(changes.Results) < numChangesExpected {
			// not enough results, retry
			return true, nil, nil
		}
		// If it made it this far, there is no errors and it got enough changes
		return false, nil, changes
	}

	return waitForChangesWorker

}

func (rt *RestTester) WaitForChanges(numChangesExpected int, changesUrl, username string, useAdminPort bool) (changes changesResults, err error) {

	waitForChangesWorker := rt.CreateWaitForChangesRetryWorker(numChangesExpected, changesUrl, username, useAdminPort)

	sleeper := base.CreateSleeperFunc(200, 100)

	err, changesVal := base.RetryLoop("Wait for changes", waitForChangesWorker, sleeper)
	if err != nil {
		return changes, err
	}

	if changesVal == nil {
		return changes, fmt.Errorf("Got nil value for changes")
	}

	if changesVal != nil {
		changes = changesVal.(changesResults)
	}

	return changes, nil
}

func (rt *RestTester) SendAdminRequest(method, resource string, body string) *TestResponse {
	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	response := &TestResponse{httptest.NewRecorder(), request}
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188

	rt.TestAdminHandler().ServeHTTP(response, request)
	return response
}

func (rt *RestTester) WaitForNUserViewResults(numResultsExpected int, viewUrlPath string, user auth.User, password string) (viewResult sgbucket.ViewResult, err error) {
	return rt.WaitForNViewResults(numResultsExpected, viewUrlPath, user, password)
}

func (rt *RestTester) WaitForNAdminViewResults(numResultsExpected int, viewUrlPath string) (viewResult sgbucket.ViewResult, err error) {
	return rt.WaitForNViewResults(numResultsExpected, viewUrlPath, nil, "")
}

// Wait for a certain number of results to be returned from a view query
// viewUrlPath: is the path to the view, including the db name.  Eg: "/db/_design/foo/_view/bar"
func (rt *RestTester) WaitForNViewResults(numResultsExpected int, viewUrlPath string, user auth.User, password string) (viewResult sgbucket.ViewResult, err error) {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		var response *TestResponse
		if user != nil {
			request, _ := http.NewRequest("GET", viewUrlPath, nil)
			request.SetBasicAuth(user.Name(), password)
			response = rt.Send(request)
		} else {
			response = rt.SendAdminRequest("GET", viewUrlPath, ``)
		}

		// If the view is undefined, it might be a race condition where the view is still being created
		// See https://github.com/couchbase/sync_gateway/issues/3570#issuecomment-390487982
		if strings.Contains(response.Body.String(), "view_undefined") {
			base.Infof(base.KeyAll, "view_undefined error: %v.  Retrying", response.Body.String())
			return true, nil, nil
		}

		if response.Code != 200 {
			return false, fmt.Errorf("Got response code: %d from view call.  Expected 200.", response.Code), sgbucket.ViewResult{}
		}
		var result sgbucket.ViewResult
		base.JSONUnmarshal(response.Body.Bytes(), &result)

		if len(result.Rows) >= numResultsExpected {
			// Got enough results, break out of retry loop
			return false, nil, result
		}

		// Not enough results, retry
		return true, nil, sgbucket.ViewResult{}

	}

	description := fmt.Sprintf("Wait for %d view results for query to %v", numResultsExpected, viewUrlPath)
	sleeper := base.CreateSleeperFunc(200, 100)
	err, returnVal := base.RetryLoop(description, worker, sleeper)

	if err != nil {
		return sgbucket.ViewResult{}, err
	}

	return returnVal.(sgbucket.ViewResult), nil

}

// Waits for view to be defined on the server.  Used to avoid view_undefined errors.
func (rt *RestTester) WaitForViewAvailable(viewURLPath string) (err error) {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		response := rt.SendAdminRequest("GET", viewURLPath, ``)

		if response.Code == 200 {
			return false, nil, nil
		}

		// Views unavailable, retry
		if response.Code == 500 {
			log.Printf("Error waiting for view to be available....will retry: %s", response.Body.Bytes())
			return true, fmt.Errorf("500 error"), nil
		}

		// Unexpected error, return
		return false, fmt.Errorf("Unexpected error response code while waiting for view available: %v", response.Code), nil

	}

	description := "Wait for view readiness"
	sleeper := base.CreateSleeperFunc(200, 100)
	err, _ = base.RetryLoop(description, worker, sleeper)

	return err

}

func (rt *RestTester) WaitForDBOnline() (err error) {

	maxTries := 20

	for i := 0; i < maxTries; i++ {

		response := rt.SendAdminRequest("GET", "/db/", "")
		var body db.Body
		base.JSONUnmarshal(response.Body.Bytes(), &body)

		if body["state"].(string) == "Online" {
			return
		}

		// Otherwise, sleep and try again
		time.Sleep(500 * time.Millisecond)

	}

	return fmt.Errorf("Give up waiting for DB to come online after %d attempts", maxTries)

}

func (rt *RestTester) SendAdminRequestWithHeaders(method, resource string, body string, headers map[string]string) *TestResponse {
	input := bytes.NewBufferString(body)
	request, _ := http.NewRequest(method, "http://localhost"+resource, input)
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	response := &TestResponse{httptest.NewRecorder(), request}
	response.Code = 200 // doesn't seem to be initialized by default; filed Go bug #4188

	rt.TestAdminHandler().ServeHTTP(response, request)
	return response
}

type SimpleSync struct {
	Channels map[string]interface{}
	Rev      string
	Sequence uint64
}

type RawResponse struct {
	Sync SimpleSync `json:"_sync"`
}

// GetDocumentSequence looks up the sequence for a document using the _raw endpoint.
// Used by tests that need to validate sequences (for grants, etc)
func (rt *RestTester) GetDocumentSequence(key string) (sequence uint64) {
	response := rt.SendAdminRequest("GET", fmt.Sprintf("/db/_raw/%s", key), "")
	if response.Code != 200 {
		return 0
	}

	var rawResponse RawResponse
	base.JSONUnmarshal(response.Body.Bytes(), &rawResponse)
	return rawResponse.Sync.Sequence
}

type TestResponse struct {
	*httptest.ResponseRecorder
	Req *http.Request
}

func (r TestResponse) DumpBody() {
	log.Printf("%v", string(r.Body.Bytes()))
}

func (r TestResponse) GetRestDocument() RestDocument {
	restDoc := NewRestDocument()
	err := base.JSONUnmarshal(r.Body.Bytes(), restDoc)
	if err != nil {
		panic(fmt.Sprintf("Error parsing body into RestDocument.  Body: %s.  Err: %v", string(r.Body.Bytes()), err))
	}
	return *restDoc
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

func assertStatus(t *testing.T, response *TestResponse, expectedStatus int) {
	require.Equalf(t, expectedStatus, response.Code,
		"Response status %d %q (expected %d %q)\nfor %s <%s> : %s",
		response.Code, http.StatusText(response.Code),
		expectedStatus, http.StatusText(expectedStatus),
		response.Req.Method, response.Req.URL, response.Body)
}

func NewSlowResponseRecorder(responseDelay time.Duration, responseRecorder *httptest.ResponseRecorder) *SlowResponseRecorder {

	responseStarted := sync.WaitGroup{}
	responseStarted.Add(1)

	responseFinished := sync.WaitGroup{}
	responseFinished.Add(1)

	return &SlowResponseRecorder{
		responseDelay:    responseDelay,
		ResponseRecorder: responseRecorder,
		responseStarted:  &responseStarted,
		responseFinished: &responseFinished,
	}

}

type SlowResponseRecorder struct {
	*httptest.ResponseRecorder
	responseDelay    time.Duration
	responseStarted  *sync.WaitGroup
	responseFinished *sync.WaitGroup
}

func (s *SlowResponseRecorder) WaitForResponseToStart() {
	s.responseStarted.Wait()
}

func (s *SlowResponseRecorder) WaitForResponseToFinish() {
	s.responseFinished.Wait()
}

func (s *SlowResponseRecorder) Write(buf []byte) (int, error) {

	s.responseStarted.Done()

	time.Sleep(s.responseDelay)

	numBytesWritten, err := s.ResponseRecorder.Write(buf)

	s.responseFinished.Done()

	return numBytesWritten, err
}

// The parameters used to create a BlipTester
type BlipTesterSpec struct {

	// Run Sync Gateway in "No conflicts" mode.  Will be propgated to the underyling RestTester
	noConflictsMode bool

	// If an underlying RestTester is created, it will propagate this setting to the underlying RestTester.
	noAdminParty bool

	// The Sync Gateway username and password to connect with.  If set, then you
	// may want to disable "Admin Party" mode, which will allow guest user access.
	// By default, the created user will have access to a single channel that matches their username.
	// If you need to grant the user access to more channels, you can override this behavior with the
	// connectingUserChannelGrants field
	connectingUsername string
	connectingPassword string

	// By default, the created user will have access to a single channel that matches their username.
	// If you need to grant the user access to more channels, you can override this behavior by specifying
	// the channels the user should have access in this string slice
	connectingUserChannelGrants []string

	// Allow tests to further customized a RestTester or re-use it across multiple BlipTesters if needed.
	// If a RestTester is passed in, certain properties of the BlipTester such as noAdminParty will be ignored, since
	// those properties only affect the creation of the RestTester.
	// If nil, a default restTester will be created based on the properties in this spec
	restTester *RestTester
}

// State associated with a BlipTester
// Note that it's not safe to have multiple goroutines access a single BlipTester due to the
// fact that certain methods register profile handlers on the BlipContext
type BlipTester struct {

	// The underlying RestTester which is used to bootstrap the initial blip websocket creation,
	// as well as providing a way for tests to access Sync Gateway over REST to hit admin-only endpoints
	// which are not available via blip.  Since a test may need to create multiple BlipTesters for multiple
	// user contexts, a single RestTester may be shared among multiple BlipTester instances.
	restTester *RestTester

	// The blip context which contains blip related state and the sender/reciever goroutines associated
	// with this websocket connection
	blipContext *blip.Context

	// The blip sender that can be used for sending messages over the websocket connection
	sender *blip.Sender
}

// Close the bliptester
func (bt BlipTester) Close() {
	bt.restTester.Close()
}

// Returns database context for blipTester (assumes underlying rest tester is based on a single db - returns first it finds)
func (bt BlipTester) DatabaseContext() *db.DatabaseContext {
	dbs := bt.restTester.ServerContext().AllDatabases()
	for _, database := range dbs {
		return database
	}
	return nil
}

// Create a BlipTester using the default spec
func NewBlipTester(tb testing.TB) (*BlipTester, error) {
	defaultSpec := BlipTesterSpec{}
	return NewBlipTesterFromSpec(tb, defaultSpec)
}

// Create a BlipTester using the given spec
func NewBlipTesterFromSpec(tb testing.TB, spec BlipTesterSpec) (*BlipTester, error) {

	bt := &BlipTester{}

	if spec.restTester != nil {
		bt.restTester = spec.restTester
	} else {
		rtConfig := RestTesterConfig{
			EnableNoConflictsMode: spec.noConflictsMode,
			noAdminParty:          spec.noAdminParty,
		}
		var rt = NewRestTester(tb, &rtConfig)
		bt.restTester = rt
	}

	// Since blip requests all go over the public handler, wrap the public handler with the httptest server
	publicHandler := bt.restTester.TestPublicHandler()

	if len(spec.connectingUsername) > 0 {

		// By default, the user will be granted access to a single channel equal to their username
		adminChannels := []string{spec.connectingUsername}

		// If the caller specified a list of channels to grant the user access to, then use that instead.
		if len(spec.connectingUserChannelGrants) > 0 {
			adminChannels = []string{} // empty it
			adminChannels = append(adminChannels, spec.connectingUserChannelGrants...)
		}

		// serialize admin channels to json array
		adminChannelsJson, err := base.JSONMarshal(adminChannels)
		if err != nil {
			return nil, err
		}
		adminChannelsStr := fmt.Sprintf("%s", adminChannelsJson)

		userDocBody := fmt.Sprintf(`{"name":"%s", "password":"%s", "admin_channels":%s}`,
			spec.connectingUsername,
			spec.connectingPassword,
			adminChannelsStr,
		)
		log.Printf("Creating user: %v", userDocBody)

		// Create a user.  NOTE: this must come *after* the bt.rt.TestPublicHandler() call, otherwise it will end up getting ignored
		_ = bt.restTester.SendAdminRequest(
			"POST",
			"/db/_user/",
			userDocBody,
		)
	}

	// Create a _temporary_ test server bound to an actual port that is used to make the blip connection.
	// This is needed because the mock-based approach fails with a "Connection not hijackable" error when
	// trying to do the websocket upgrade.  Since it's only needed to setup the websocket, it can be closed
	// as soon as the websocket is established, hence the defer srv.Close() call.
	srv := httptest.NewServer(publicHandler)
	defer srv.Close()

	// Construct URL to connect to blipsync target endpoint
	destUrl := fmt.Sprintf("%s/db/_blipsync", srv.URL)
	u, err := url.Parse(destUrl)
	if err != nil {
		return nil, err
	}
	u.Scheme = "ws"

	// Make BLIP/Websocket connection
	bt.blipContext = blip.NewContext(BlipCBMobileReplication)
	bt.blipContext.Logger = DefaultBlipLogger(
		context.WithValue(context.Background(), base.LogContextKey{},
			base.LogContext{CorrelationID: base.FormatBlipContextID(bt.blipContext.ID)},
		),
	)

	bt.blipContext.LogMessages = base.LogDebugEnabled(base.KeyWebSocket)
	bt.blipContext.LogFrames = base.LogDebugEnabled(base.KeyWebSocketFrame)

	origin := "http://localhost" // TODO: what should be used here?

	config, err := websocket.NewConfig(u.String(), origin)
	if err != nil {
		return nil, err
	}

	if len(spec.connectingUsername) > 0 {
		config.Header = http.Header{
			"Authorization": {"Basic " + base64.StdEncoding.EncodeToString([]byte(spec.connectingUsername+":"+spec.connectingPassword))},
		}
	}

	bt.sender, err = bt.blipContext.DialConfig(config)
	if err != nil {
		return nil, err
	}

	return bt, nil

}

func (bt *BlipTester) SetCheckpoint(client string, checkpointRev string, body []byte) (sent bool, req *SetCheckpointMessage, res *SetCheckpointResponse, err error) {

	scm := NewSetCheckpointMessage()
	scm.SetCompressed(true)
	scm.setClient(client)
	scm.setRev(checkpointRev)
	scm.SetBody(body)

	sent = bt.sender.Send(scm.Message)
	if !sent {
		return sent, scm, nil, fmt.Errorf("Failed to send setCheckpoint for client: %v", client)
	}

	scr := &SetCheckpointResponse{scm.Response()}
	return true, scm, scr, nil

}

// The docHistory should be in the same format as expected by db.PutExistingRevWithBody(), or empty if this is the first revision
func (bt *BlipTester) SendRevWithHistory(docId, docRev string, revHistory []string, body []byte, properties blip.Properties) (sent bool, req, res *blip.Message, err error) {

	revRequest := blip.NewRequest()
	revRequest.SetCompressed(true)
	revRequest.SetProfile("rev")

	revRequest.Properties["id"] = docId
	revRequest.Properties["rev"] = docRev
	revRequest.Properties["deleted"] = "false"
	if len(revHistory) > 0 {
		revRequest.Properties["history"] = strings.Join(revHistory, ",")
	}

	// Override any properties which have been supplied explicitly
	for k, v := range properties {
		revRequest.Properties[k] = v
	}

	revRequest.SetBody(body)
	sent = bt.sender.Send(revRequest)
	if !sent {
		return sent, revRequest, nil, fmt.Errorf("Failed to send revRequest for doc: %v", docId)
	}
	revResponse := revRequest.Response()
	if revResponse.SerialNumber() != revRequest.SerialNumber() {
		return sent, revRequest, revResponse, fmt.Errorf("revResponse.SerialNumber() != revRequest.SerialNumber().  %v != %v", revResponse.SerialNumber(), revRequest.SerialNumber())
	}

	// Make sure no errors.  Just panic for now, but if there are tests that expect errors and want
	// to use SendRev(), this could be returned.
	if errorCode, ok := revResponse.Properties["Error-Code"]; ok {
		body, _ := revResponse.Body()
		return sent, revRequest, revResponse, fmt.Errorf("Unexpected error sending rev: %v\n%s", errorCode, body)
	}

	return sent, revRequest, revResponse, nil

}

func (bt *BlipTester) SendRev(docId, docRev string, body []byte, properties blip.Properties) (sent bool, req, res *blip.Message, err error) {

	return bt.SendRevWithHistory(docId, docRev, []string{}, body, properties)

}

// Get a doc at a particular revision from Sync Gateway.
//
// Warning: this can only be called from a single goroutine, given the fact it registers profile handlers.
//
// If that is not found, it will return an empty resultDoc with no errors.
//
// - Call subChanges (continuous=false) endpoint to get all changes from Sync Gateway
// - Respond to each "change" request telling the other side to send the revision
//		- NOTE: this could be made more efficient by only requesting the revision for the docid/revid pair
//              passed in the parameter.
// - If the rev handler is called back with the desired docid/revid pair, save that into a variable that will be returned
// - Block until all pending operations are complete
// - Return the resultDoc or an empty resultDoc
//
func (bt *BlipTester) GetDocAtRev(requestedDocID, requestedDocRev string) (resultDoc RestDocument, err error) {

	docs := map[string]RestDocument{}
	changesFinishedWg := sync.WaitGroup{}
	revsFinishedWg := sync.WaitGroup{}

	defer func() {
		// Clean up all profile handlers that are registered as part of this test
		delete(bt.blipContext.HandlerForProfile, "changes")
		delete(bt.blipContext.HandlerForProfile, "rev")
	}()

	// -------- Changes handler callback --------
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		// Send a response telling the other side we want ALL revisions

		body, err := request.Body()
		if err != nil {
			panic(fmt.Sprintf("Error getting request body: %v", err))
		}

		if string(body) == "null" {
			changesFinishedWg.Done()
			return
		}

		if !request.NoReply() {

			// unmarshal into json array
			changesBatch := [][]interface{}{}

			if err := base.JSONUnmarshal(body, &changesBatch); err != nil {
				panic(fmt.Sprintf("Error unmarshalling changes. Body: %vs.  Error: %v", string(body), err))
			}

			responseVal := [][]interface{}{}
			for _, change := range changesBatch {
				revId := change[2].(string)
				responseVal = append(responseVal, []interface{}{revId})
				revsFinishedWg.Add(1)
			}

			response := request.Response()
			responseValBytes, err := base.JSONMarshal(responseVal)
			log.Printf("responseValBytes: %s", responseValBytes)
			if err != nil {
				panic(fmt.Sprintf("Error marshalling response: %v", err))
			}
			response.SetBody(responseValBytes)

		}
	}

	// -------- Rev handler callback --------
	bt.blipContext.HandlerForProfile["rev"] = func(request *blip.Message) {

		defer revsFinishedWg.Done()
		body, err := request.Body()
		var doc RestDocument
		err = base.JSONUnmarshal(body, &doc)
		if err != nil {
			panic(fmt.Sprintf("Unexpected err: %v", err))
		}
		docId := request.Properties["id"]
		docRev := request.Properties["rev"]
		doc.SetID(docId)
		doc.SetRevID(docRev)
		docs[docId] = doc

		if docId == requestedDocID && docRev == requestedDocRev {
			resultDoc = doc
		}

	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	changesFinishedWg.Add(1)
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"

	sent := bt.sender.Send(subChangesRequest)
	if !sent {
		panic(fmt.Sprintf("Unable to subscribe to changes."))
	}

	changesFinishedWg.Wait()
	revsFinishedWg.Wait()

	return resultDoc, nil

}

type SendRevWithAttachmentInput struct {
	docId            string
	revId            string
	attachmentName   string
	attachmentLength int
	attachmentBody   string
	attachmentDigest string
}

// Warning: this can only be called from a single goroutine, given the fact it registers profile handlers.
func (bt *BlipTester) SendRevWithAttachment(input SendRevWithAttachmentInput) (sent bool, req, res *blip.Message) {

	defer func() {
		// Clean up all profile handlers that are registered as part of this test
		delete(bt.blipContext.HandlerForProfile, "getAttachment")
	}()

	// Create a doc with an attachment
	myAttachment := db.DocAttachment{
		ContentType: "application/json",
		Digest:      input.attachmentDigest,
		Length:      input.attachmentLength,
		Revpos:      1,
		Stub:        true,
	}
	doc := NewRestDocument()
	doc.SetID(input.docId)
	doc.SetRevID(input.revId)
	doc.SetAttachments(db.AttachmentMap{
		input.attachmentName: &myAttachment,
	})

	docBody, err := base.JSONMarshal(doc)
	if err != nil {
		panic(fmt.Sprintf("Error marshalling doc.  Error: %v", err))
	}

	getAttachmentWg := sync.WaitGroup{}

	bt.blipContext.HandlerForProfile["getAttachment"] = func(request *blip.Message) {
		defer getAttachmentWg.Done()
		if request.Properties["digest"] != myAttachment.Digest {
			panic(fmt.Sprintf("Unexpected digest.  Got: %v, expected: %v", request.Properties["digest"], myAttachment.Digest))
		}
		response := request.Response()
		response.SetBody([]byte(input.attachmentBody))
	}

	// Push a rev with an attachment.
	getAttachmentWg.Add(1)
	sent, req, res, _ = bt.SendRev(
		input.docId,
		input.revId,
		docBody,
		blip.Properties{},
	)

	// Expect a callback to the getAttachment endpoint
	getAttachmentWg.Wait()

	return sent, req, res

}

func (bt *BlipTester) WaitForNumChanges(numChangesExpected int) (changes [][]interface{}) {

	retryWorker := func() (shouldRetry bool, err error, value interface{}) {
		currentChanges := bt.GetChanges()
		if len(currentChanges) >= numChangesExpected {
			return false, nil, currentChanges
		}

		// haven't seen numDocsExpected yet, so wait and retry
		return true, nil, nil

	}

	_, rawChanges := base.RetryLoop(
		"WaitForNumChanges",
		retryWorker,
		base.CreateDoublingSleeperFunc(10, 10),
	)

	return rawChanges.([][]interface{})

}

// Returns changes in form of [[sequence, docID, revID, deleted], [sequence, docID, revID, deleted]]
// Warning: this can only be called from a single goroutine, given the fact it registers profile handlers.
func (bt *BlipTester) GetChanges() (changes [][]interface{}) {

	defer func() {
		// Clean up all profile handlers that are registered as part of this test
		delete(bt.blipContext.HandlerForProfile, "changes") // a handler for this profile is registered in SubscribeToChanges
	}()

	collectedChanges := [][]interface{}{}
	chanChanges := make(chan *blip.Message)
	bt.SubscribeToChanges(false, chanChanges)

	for changeMsg := range chanChanges {

		body, err := changeMsg.Body()
		if err != nil {
			panic(fmt.Sprintf("Error getting request body: %v", err))
		}

		if string(body) == "null" {
			// the other side indicated that it's done sending changes.
			// this only works (I think) because continuous=false.
			close(chanChanges)
			break
		}

		// unmarshal into json array
		changesBatch := [][]interface{}{}

		if err := base.JSONUnmarshal(body, &changesBatch); err != nil {
			panic(fmt.Sprintf("Error unmarshalling changes. Body: %vs.  Error: %v", string(body), err))
		}

		for _, change := range changesBatch {
			collectedChanges = append(collectedChanges, change)
		}

	}

	return collectedChanges

}

func (bt *BlipTester) WaitForNumDocsViaChanges(numDocsExpected int) (docs map[string]RestDocument) {

	retryWorker := func() (shouldRetry bool, err error, value interface{}) {
		allDocs := bt.PullDocs()
		if len(allDocs) >= numDocsExpected {
			return false, nil, allDocs
		}

		// haven't seen numDocsExpected yet, so wait and retry
		return true, nil, nil

	}

	_, allDocs := base.RetryLoop(
		"WaitForNumDocsViaChanges",
		retryWorker,
		base.CreateDoublingSleeperFunc(10, 10),
	)

	return allDocs.(map[string]RestDocument)

}

// Get all documents and their attachments via the following steps:
//
// - Invoking one-shot subChanges request
// - Responding to all incoming "changes" requests from peer to request the changed rev, and accumulate rev body
// - Responding to all incoming "rev" requests from peer to get all attachments, and accumulate them
// - Return accumulated docs + attachements to caller
//
// It is basically a pull replication without the checkpointing
// Warning: this can only be called from a single goroutine, given the fact it registers profile handlers.
func (bt *BlipTester) PullDocs() (docs map[string]RestDocument) {

	docs = map[string]RestDocument{}

	// Mutex to avoid write contention on docs while PullDocs is running (as rev messages may be processed concurrently)
	var docsLock sync.Mutex
	changesFinishedWg := sync.WaitGroup{}
	revsFinishedWg := sync.WaitGroup{}

	defer func() {
		// Clean up all profile handlers that are registered as part of this test
		delete(bt.blipContext.HandlerForProfile, "changes")
		delete(bt.blipContext.HandlerForProfile, "rev")
	}()

	// -------- Changes handler callback --------
	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		// Send a response telling the other side we want ALL revisions

		body, err := request.Body()
		if err != nil {
			panic(fmt.Sprintf("Error getting request body: %v", err))
		}

		if string(body) == "null" {
			changesFinishedWg.Done()
			return
		}

		if !request.NoReply() {

			// unmarshal into json array
			changesBatch := [][]interface{}{}

			if err := base.JSONUnmarshal(body, &changesBatch); err != nil {
				panic(fmt.Sprintf("Error unmarshalling changes. Body: %vs.  Error: %v", string(body), err))
			}

			responseVal := [][]interface{}{}
			for _, change := range changesBatch {
				revId := change[2].(string)
				responseVal = append(responseVal, []interface{}{revId})
				revsFinishedWg.Add(1)
			}

			response := request.Response()
			responseValBytes, err := base.JSONMarshal(responseVal)
			log.Printf("responseValBytes: %s", responseValBytes)
			if err != nil {
				panic(fmt.Sprintf("Error marshalling response: %v", err))
			}
			response.SetBody(responseValBytes)

		}
	}

	// -------- Rev handler callback --------
	bt.blipContext.HandlerForProfile["rev"] = func(request *blip.Message) {

		defer revsFinishedWg.Done()
		body, err := request.Body()
		var doc RestDocument
		err = base.JSONUnmarshal(body, &doc)
		if err != nil {
			panic(fmt.Sprintf("Unexpected err: %v", err))
		}
		docId := request.Properties["id"]
		docRev := request.Properties["rev"]
		doc.SetID(docId)
		doc.SetRevID(docRev)

		docsLock.Lock()
		docs[docId] = doc
		docsLock.Unlock()

		attachments, err := doc.GetAttachments()
		if err != nil {
			panic(fmt.Sprintf("Unexpected err: %v", err))
		}

		for _, attachment := range attachments {

			// Get attachments and append to RestDocument
			getAttachmentRequest := blip.NewRequest()
			getAttachmentRequest.SetProfile("getAttachment")
			getAttachmentRequest.Properties["digest"] = attachment.Digest
			sent := bt.sender.Send(getAttachmentRequest)
			if !sent {
				panic(fmt.Sprintf("Unable to get attachment."))
			}
			getAttachmentResponse := getAttachmentRequest.Response()
			getAttachmentBody, getAttachmentErr := getAttachmentResponse.Body()
			if getAttachmentErr != nil {
				panic(fmt.Sprintf("Unexpected err: %v", err))
			}
			log.Printf("getAttachmentBody: %s", getAttachmentBody)
			attachment.Data = getAttachmentBody
		}

		// Send response to rev request
		if !request.NoReply() {
			response := request.Response()
			response.SetBody([]byte{}) // Empty response to indicate success
		}

	}

	// -------- Norev handler callback --------
	bt.blipContext.HandlerForProfile["norev"] = func(request *blip.Message) {
		// If a norev is received, then don't bother waiting for one of the expected revisions, since it will never come.
		// The norev could be added to the returned docs map, but so far there is no need for that.  The ability
		// to assert on the number of actually received revisions (which norevs won't affect) meets current test requirements.
		defer revsFinishedWg.Done()
	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	changesFinishedWg.Add(1)
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"

	sent := bt.sender.Send(subChangesRequest)
	if !sent {
		panic(fmt.Sprintf("Unable to subscribe to changes."))
	}

	changesFinishedWg.Wait()

	revsFinishedWg.Wait()

	return docs

}

func (bt *BlipTester) SubscribeToChanges(continuous bool, changes chan<- *blip.Message) {

	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		changes <- request

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []interface{}{}
			emptyResponseValBytes, err := base.JSONMarshal(emptyResponseVal)
			if err != nil {
				panic(fmt.Sprintf("Error marshalling response: %v", err))
			}
			response.SetBody(emptyResponseValBytes)
		}

	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	switch continuous {
	case true:
		subChangesRequest.Properties["continuous"] = "true"
	default:
		subChangesRequest.Properties["continuous"] = "false"
	}

	sent := bt.sender.Send(subChangesRequest)
	if !sent {
		panic(fmt.Sprintf("Unable to subscribe to changes."))
	}
	subChangesResponse := subChangesRequest.Response()
	if subChangesResponse.SerialNumber() != subChangesRequest.SerialNumber() {
		panic(fmt.Sprintf("subChangesResponse.SerialNumber() != subChangesRequest.SerialNumber().  %v != %v", subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber()))
	}

}

// Helper for comparing BLIP changes received with expected BLIP changes
type ExpectedChange struct {
	docId    string // DocId or "*" for any doc id
	revId    string // RevId or "*" for any rev id
	sequence string // Sequence or "*" for any sequence
	deleted  *bool  // Deleted status or nil for any deleted status
}

func (e ExpectedChange) Equals(change []interface{}) error {

	// TODO: this is commented because it's giving an error: panic: interface conversion: interface {} is float64, not string [recovered].
	// TODO: I think this should be addressed by adding a BlipChange struct stronger typing than a slice of empty interfaces.  TBA.
	// changeSequence := change[0].(string)

	var changeDeleted *bool

	changeDocId := change[1].(string)
	changeRevId := change[2].(string)
	if len(change) > 3 {
		changeDeletedVal := change[3].(bool)
		changeDeleted = &changeDeletedVal
	}

	if e.docId != "*" && changeDocId != e.docId {
		return fmt.Errorf("changeDocId (%s) != expectedChangeDocId (%s)", changeDocId, e.docId)
	}

	if e.revId != "*" && changeRevId != e.revId {
		return fmt.Errorf("changeRevId (%s) != expectedChangeRevId (%s)", changeRevId, e.revId)
	}

	// TODO: commented due to reasons given above
	//if e.sequence != "*" && changeSequence != e.sequence {
	//	return fmt.Errorf("changeSequence (%s) != expectedChangeSequence (%s)", changeSequence, e.sequence)
	//}

	if changeDeleted != nil && e.deleted != nil && *changeDeleted != *e.deleted {
		return fmt.Errorf("changeDeleted (%v) != expectedChangeDeleted (%v)", *changeDeleted, *e.deleted)
	}

	return nil
}

// Model "CouchDB" style REST documents which define the following special fields:
//
// - _id
// - _rev
// - _removed
// - _deleted (not accounted for yet)
// - _attachments
//
// This struct wraps a map and provides convenience methods for getting at the special
// fields with the appropriate types (string in the id/rev case, db.AttachmentMap in the attachments case).
// Currently only used in tests, but if similar functionality needed in primary codebase, could be moved.
type RestDocument map[string]interface{}

func NewRestDocument() *RestDocument {
	emptyBody := make(map[string]interface{})
	restDoc := RestDocument(emptyBody)
	return &restDoc
}

func (d RestDocument) ID() string {
	rawID, hasID := d[db.BodyId]
	if !hasID {
		return ""
	}
	return rawID.(string)

}

func (d RestDocument) SetID(docId string) {
	d[db.BodyId] = docId
}

func (d RestDocument) RevID() string {
	rawRev, hasRev := d[db.BodyRev]
	if !hasRev {
		return ""
	}
	return rawRev.(string)
}

func (d RestDocument) SetRevID(revId string) {
	d[db.BodyRev] = revId
}

func (d RestDocument) SetAttachments(attachments db.AttachmentMap) {
	d[db.BodyAttachments] = attachments
}

func (d RestDocument) GetAttachments() (db.AttachmentMap, error) {

	rawAttachments, hasAttachments := d[db.BodyAttachments]

	// If the map doesn't even have the _attachments key, return an empty attachments map
	if !hasAttachments {
		return db.AttachmentMap{}, nil
	}

	// Otherwise, create an AttachmentMap from the value in the raw map
	attachmentMap := db.AttachmentMap{}
	switch v := rawAttachments.(type) {
	case db.AttachmentMap:
		// If it's already an AttachmentMap (maybe due to previous call to SetAttachments), then return as-is
		return v, nil
	default:
		rawAttachmentsMap := v.(map[string]interface{})
		for attachmentName, attachmentVal := range rawAttachmentsMap {

			// marshal attachmentVal into a byte array, then unmarshal into a DocAttachment
			attachmentValMarshalled, err := base.JSONMarshal(attachmentVal)
			if err != nil {
				return db.AttachmentMap{}, err
			}
			docAttachment := db.DocAttachment{}
			if err := base.JSONUnmarshal(attachmentValMarshalled, &docAttachment); err != nil {
				return db.AttachmentMap{}, err
			}

			attachmentMap[attachmentName] = &docAttachment
		}

		// Avoid the unnecessary re-Marshal + re-Unmarshal
		d.SetAttachments(attachmentMap)
	}

	return attachmentMap, nil

}

func (d RestDocument) IsRemoved() bool {
	removed, ok := d[db.BodyRemoved]
	if !ok {
		return false
	}
	return removed.(bool) == true
}

// Wait for the WaitGroup, or return an error if the wg.Wait() doesn't return within timeout
func WaitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) error {

	// Create a channel so that a goroutine waiting on the waitgroup can send it's result (if any)
	wgFinished := make(chan bool)

	go func() {
		wg.Wait()
		wgFinished <- true
	}()

	select {
	case <-wgFinished:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("Timed out waiting after %v", timeout)
	}

}

type TestLogger struct {
	T *testing.T
}

func (l TestLogger) Logf(logLevel base.LogLevel, logKey base.LogKey, format string, args ...interface{}) {
	l.T.Logf(
		logLevel.String()+" "+
			logKey.String()+": "+
			format, args...,
	)
}
