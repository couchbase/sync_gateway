package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"encoding/base64"
	"net/url"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/go.assert"
	"golang.org/x/net/websocket"
)

// Testing utilities that have been included in the rest package so that they
// are available to any package that imports rest.  (if they were in a _test.go
// file, they wouldn't be publicly exported to other packages)

var gBucketCounter = 0

type RestTester struct {
	RestTesterBucket        base.Bucket
	RestTesterServerContext *ServerContext
	noAdminParty            bool      // Unless this is true, Admin Party is in full effect
	distributedIndex        bool      // Test with walrus-based index bucket
	SyncFn                  string    // put the sync() function source in here (optional)
	DatabaseConfig          *DbConfig // Supports additional config options.  BucketConfig, Name, Sync, Unsupported will be ignored (overridden)
	AdminHandler            http.Handler
	PublicHandler           http.Handler
	leakyBucketConfig       *base.LeakyBucketConfig
	EnableNoConflictsMode   bool // Enable no-conflicts mode.  By default, conflicts will be allowed, which is the default behavior
	NoFlush                 bool // Skip bucket flush step during creation.  Used by tests that need to simulate start/stop of Sync Gateway with backing bucket intact.
}

func (rt *RestTester) Bucket() base.Bucket {

	if rt.RestTesterBucket == nil {

		// Initialize the bucket.  For couchbase-backed tests, triggers with creation/flushing of the bucket
		if !rt.NoFlush {
			tempBucket := base.GetTestBucketOrPanic() // side effect of creating/flushing bucket
			tempBucket.Close()
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
			rt.DatabaseConfig = &DbConfig{}
		}

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
			panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
		}
		rt.RestTesterBucket = rt.RestTesterServerContext.Database("db").Bucket

		if !rt.noAdminParty {
			rt.SetAdminParty(true)
		}

		// If given a leakyBucketConfig we'll return a leaky bucket.
		if rt.leakyBucketConfig != nil {
			return base.NewLeakyBucket(rt.RestTesterBucket, *rt.leakyBucketConfig)
		}

	}
	return rt.RestTesterBucket
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
	})

	if err != nil {
		panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
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
	assert.True(t, err == nil)

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

func (rt *RestTester) DisableGuestUser() {
	rt.SetAdminParty(false)
}

func (rt *RestTester) Close() {
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
	if rt.AdminHandler == nil {
		rt.AdminHandler = CreateAdminHandler(rt.ServerContext())
	}
	return rt.AdminHandler
}

func (rt *RestTester) TestAdminHandler() http.Handler {
	if rt.AdminHandler == nil {
		rt.AdminHandler = CreateAdminHandler(rt.ServerContext())
	}
	return rt.AdminHandler
}

func (rt *RestTester) TestPublicHandler() http.Handler {
	if rt.PublicHandler == nil {
		rt.PublicHandler = CreatePublicHandler(rt.ServerContext())
	}
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
		err = json.Unmarshal(response.Body.Bytes(), &changes)
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
		if response.Code != 200 {
			return false, fmt.Errorf("Got response code: %d from view call.  Expected 200.", response.Code), sgbucket.ViewResult{}
		}
		var result sgbucket.ViewResult
		json.Unmarshal(response.Body.Bytes(), &result)

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

func (rt *RestTester) WaitForDBOnline() (err error) {

	maxTries := 20

	for i := 0; i < maxTries; i++ {

		response := rt.SendAdminRequest("GET", "/db/", "")
		var body db.Body
		json.Unmarshal(response.Body.Bytes(), &body)

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

type TestResponse struct {
	*httptest.ResponseRecorder
	Req *http.Request
}

func (r TestResponse) DumpBody() {
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

func assertStatus(t *testing.T, response *TestResponse, expectedStatus int) {
	if response.Code != expectedStatus {
		debug.PrintStack()
		t.Fatalf("Response status %d (expected %d) for %s <%s> : %s",
			response.Code, expectedStatus, response.Req.Method, response.Req.URL, response.Body)
	}
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

// Create a BlipTester using the default spec
func NewBlipTester() (*BlipTester, error) {
	defaultSpec := BlipTesterSpec{}
	return NewBlipTesterFromSpec(defaultSpec)
}

// Create a BlipTester using the given spec
func NewBlipTesterFromSpec(spec BlipTesterSpec) (*BlipTester, error) {

	EnableBlipSyncLogs()

	bt := &BlipTester{}

	if spec.restTester != nil {
		bt.restTester = spec.restTester
	} else {
		rt := RestTester{
			EnableNoConflictsMode: spec.noConflictsMode,
			noAdminParty:          spec.noAdminParty,
		}
		bt.restTester = &rt
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
		adminChannelsJson, err := json.Marshal(adminChannels)
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
		response := bt.restTester.SendAdminRequest(
			"POST",
			"/db/_user/",
			userDocBody,
		)
		if response.Code != 201 {
			return nil, fmt.Errorf("Expected 201 response.  Got: %v", response.Code)
		}
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
	bt.blipContext = blip.NewContext()
	bt.blipContext.Logger = func(fmt string, params ...interface{}) {
		base.LogTo("BLIP", fmt, params...)
	}
	bt.blipContext.LogMessages = true
	bt.blipContext.LogFrames = true
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

func (bt *BlipTester) SendRev(docId, docRev string, body []byte) (sent bool, req, res *blip.Message) {

	revRequest := blip.NewRequest()
	revRequest.SetCompressed(true)
	revRequest.SetProfile("rev")
	revRequest.Properties["id"] = docId
	revRequest.Properties["rev"] = docRev
	revRequest.Properties["deleted"] = "false"
	revRequest.SetBody(body)
	sent = bt.sender.Send(revRequest)
	if !sent {
		panic(fmt.Sprintf("Failed to send revRequest for doc: %v", docId))
	}
	revResponse := revRequest.Response()
	if revResponse.SerialNumber() != revRequest.SerialNumber() {
		panic(fmt.Sprintf("revResponse.SerialNumber() != revRequest.SerialNumber().  %v != %v", revResponse.SerialNumber(), revRequest.SerialNumber()))
	}

	return sent, revRequest, revResponse

}

// Returns changes in form of [[sequence, docID, revID, deleted], [sequence, docID, revID, deleted]]
func (bt *BlipTester) GetChanges() (changes [][]interface{}) {

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

		if err := json.Unmarshal(body, &changesBatch); err != nil {
			panic(fmt.Sprintf("Error unmarshalling changes. Body: %vs.  Error: %v", string(body), err))
		}

		for _, change := range changesBatch {
			collectedChanges = append(collectedChanges, change)
		}

	}

	return collectedChanges

}

func (bt *BlipTester) SubscribeToChanges(continuous bool, changes chan<- *blip.Message) {

	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		changes <- request

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []interface{}{}
			emptyResponseValBytes, err := json.Marshal(emptyResponseVal)
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

// SubscribeToChanges

func EnableBlipSyncLogs() {

	base.EnableLogKey("HTTP")
	base.EnableLogKey("HTTP+")
	base.EnableLogKey("BLIP")
	base.EnableLogKey("Sync")
	base.EnableLogKey("Sync+")

}
