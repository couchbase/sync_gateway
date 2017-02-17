//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// If set to true, JSON output will be pretty-printed.
var PrettyPrint bool = false

// If set to true, diagnostic data will be dumped if there's a problem with MIME multipart data
var DebugMultipart bool = false

var lastSerialNum uint64 = 0

var restExpvars = expvar.NewMap("syncGateway_rest")

func init() {
	DebugMultipart = (os.Getenv("GatewayDebugMultipart") != "")
}

var kNotFoundError = base.HTTPErrorf(http.StatusNotFound, "missing")
var kBadMethodError = base.HTTPErrorf(http.StatusMethodNotAllowed, "Method Not Allowed")
var kBadRequestError = base.HTTPErrorf(http.StatusMethodNotAllowed, "Bad Request")

var checkAuthRollingMean = base.NewIntRollingMeanVar(100)

func init() {
	base.StatsExpvars.Set("handler.CheckAuthRollingMean", &checkAuthRollingMean)
}

// Encapsulates the state of handling an HTTP request.
type handler struct {
	server         *ServerContext
	rq             *http.Request
	response       http.ResponseWriter
	status         int
	statusMessage  string
	requestBody    io.ReadCloser
	db             *db.Database
	user           auth.User
	privs          handlerPrivs
	startTime      time.Time
	serialNumber   uint64
	loggedDuration bool
	runOffline     bool
}

type handlerPrivs int

const (
	regularPrivs = iota // Handler requires authentication
	publicPrivs         // Handler checks auth but doesn't require it
	adminPrivs          // Handler ignores auth, always runs with root/admin privs
)

type handlerMethod func(*handler) error

// Creates an http.Handler that will run a handler with the given method
func makeHandler(server *ServerContext, privs handlerPrivs, method handlerMethod) http.Handler {
	return http.HandlerFunc(func(r http.ResponseWriter, rq *http.Request) {
		runOffline := false
		h := newHandler(server, privs, r, rq, runOffline)
		err := h.invoke(method)
		h.writeError(err)
		h.logDuration(true)
	})
}

// Creates an http.Handler that will run a handler with the given method even if the target DB is offline
func makeOfflineHandler(server *ServerContext, privs handlerPrivs, method handlerMethod) http.Handler {
	return http.HandlerFunc(func(r http.ResponseWriter, rq *http.Request) {
		runOffline := true
		h := newHandler(server, privs, r, rq, runOffline)
		err := h.invoke(method)
		h.writeError(err)
		h.logDuration(true)
	})
}

func newHandler(server *ServerContext, privs handlerPrivs, r http.ResponseWriter, rq *http.Request, runOffline bool) *handler {
	return &handler{
		server:       server,
		privs:        privs,
		rq:           rq,
		response:     r,
		status:       http.StatusOK,
		serialNumber: atomic.AddUint64(&lastSerialNum, 1),
		startTime:    time.Now(),
		runOffline:   runOffline,
	}
}

// Top-level handler call. It's passed a pointer to the specific method to run.
func (h *handler) invoke(method handlerMethod) error {
	base.StatsExpvars.Add("requests_total", 1)
	base.StatsExpvars.Add("requests_active", 1)
	defer base.StatsExpvars.Add("requests_active", -1)

	var err error
	if h.server.config.CompressResponses == nil || *h.server.config.CompressResponses {
		if encoded := NewEncodedResponseWriter(h.response, h.rq); encoded != nil {
			h.response = encoded
			defer encoded.Close()
		}
	}

	switch h.rq.Header.Get("Content-Encoding") {
	case "":
		h.requestBody = h.rq.Body
	case "gzip":
		if h.requestBody, err = gzip.NewReader(h.rq.Body); err != nil {
			return err
		}
		h.rq.Header.Del("Content-Encoding") // to prevent double decoding later on
	default:
		return base.HTTPErrorf(http.StatusUnsupportedMediaType, "Unsupported Content-Encoding; use gzip")
	}

	h.setHeader("Server", VersionString)

	// If there is a "db" path variable, look up the database context:
	var dbContext *db.DatabaseContext
	if dbname := h.PathVar("db"); dbname != "" {
		if dbContext, err = h.server.GetDatabase(dbname); err != nil {
			h.logRequestLine()
			return err
		}
	}

	// If this call is in the context of a DB make sure the DB is in a valid state
	if dbContext != nil {
		if !h.runOffline {

			//get a read lock on the dbContext
			//When the lock is returned we know that the db state will not be changed by
			//any other call
			dbContext.AccessLock.RLock()

			//defer releasing the dbContext until after the handler method returns
			defer dbContext.AccessLock.RUnlock()

			dbState := atomic.LoadUint32(&dbContext.State)

			//if dbState == db.DBOnline, continue flow and invoke the handler method
			if dbState == db.DBOffline {
				//DB is offline, only handlers with runOffline true can run in this state
				return base.HTTPErrorf(http.StatusServiceUnavailable, "DB is currently under maintenance")
			} else if dbState != db.DBOnline {
				//DB is in transition state, no calls will be accepted until it is Online or Offline state
				return base.HTTPErrorf(http.StatusServiceUnavailable, fmt.Sprintf("DB is %v - try again later", db.RunStateString[dbState]))
			}
		}
	}

	// Authenticate, if not on admin port:
	if h.privs != adminPrivs {
		if err = h.checkAuth(dbContext); err != nil {
			h.logRequestLine()
			return err
		}
	}

	h.logRequestLine()

	if base.EnableLogHTTPBodies {
		h.logRequestBody()
	}

	// Now set the request's Database (i.e. context + user)
	if dbContext != nil {
		h.db, err = db.GetDatabase(dbContext, h.user)
		if err != nil {
			return err
		}
	}

	if base.EnableLogHTTPBodies {
		// Wrap the existing ResponseWriter with one that "Tees" the output
		// to stdout as well as writing back to the socket
		h.response = NewLoggerTeeResponseWriter(
			h.response,
			"HTTP",
			h.serialNumber,
			h.rq,
		)
	}

	return method(h) // Call the actual handler code
}

func (h *handler) logRequestLine() {
	if !base.LogEnabled("HTTP") {
		return
	}
	as := ""
	if h.privs == adminPrivs {
		as = "  (ADMIN)"
	} else if h.user != nil && h.user.Name() != "" {
		as = fmt.Sprintf("  (as %s)", h.user.Name())
	}
	proto := ""
	if h.rq.ProtoMajor >= 2 {
		proto = " HTTP/2"
	}

	base.LogTo("HTTP", " #%03d: %s %s%s%s", h.serialNumber, h.rq.Method, base.SanitizeRequestURL(h.rq.URL), proto, as)
}

func (h *handler) logRequestBody() {

	if !base.EnableLogHTTPBodies {
		return
	}

	// Replace the requestBody io.ReadCloser with a TeeReadCloser that
	// tees the request body to the base.Log with the HTTPBody logging key
	h.requestBody = NewTeeReadCloser(
		h.requestBody,
		base.NewLoggerWriter(
			"HTTP",
			h.serialNumber,
			h.rq,
		),
	)

}

func (h *handler) logDuration(realTime bool) {
	if h.loggedDuration {
		return
	}
	h.loggedDuration = true

	var duration time.Duration
	if realTime {
		duration = time.Since(h.startTime)
		bin := int(duration/(100*time.Millisecond)) * 100
		restExpvars.Add(fmt.Sprintf("requests_%04dms", bin), 1)
	}

	logKey := "HTTP+"
	if h.status >= 300 {
		logKey = "HTTP"
	}
	base.LogTo(logKey, "#%03d:     --> %d %s  (%.1f ms)",
		h.serialNumber, h.status, h.statusMessage,
		float64(duration)/float64(time.Millisecond))
}

// Used for indefinitely-long handlers like _changes that we don't want to track duration of
func (h *handler) logStatus(status int, message string) {
	h.setStatus(status, message)
	h.logDuration(false) // don't track actual time
}

func (h *handler) checkAuth(context *db.DatabaseContext) error {
	h.user = nil
	if context == nil {
		return nil
	}

	defer checkAuthRollingMean.AddSince(time.Now())

	var err error
	// If oidc enabled, check for bearer ID token
	if context.Options.OIDCOptions != nil {
		if token := h.getBearerToken(); token != "" {
			h.user, _, err = context.Authenticator().AuthenticateUntrustedJWT(token, context.OIDCProviders, h.getOIDCCallbackURL)
			if h.user == nil || err != nil {
				return base.HTTPErrorf(http.StatusUnauthorized, "Invalid login")
			}
			return nil
		}

		/*
		* If unsupported/oidc testing is enabled
		* and this is a call on the token endpoint
		* and the username and password match those in the oidc default provider config
		* then authorize this request
		 */
		if unsupportedOptions := context.Options.UnsupportedOptions; unsupportedOptions != nil {
			if unsupportedOptions.OidcTestProvider.Enabled && strings.HasSuffix(h.rq.URL.Path, "/_oidc_testing/token") {
				if username, password := h.getBasicAuth(); username != "" && password != "" {
					provider := context.Options.OIDCOptions.Providers.GetProviderForIssuer(issuerUrlForDB(h, context.Name), testProviderAudiences)
					if provider != nil && provider.ClientID != nil && provider.ValidationKey != nil {
						if *provider.ClientID == username && *provider.ValidationKey == password {
							return nil
						}
					}
				}
			}
		}
	}

	// Check basic auth first
	if userName, password := h.getBasicAuth(); userName != "" {
		h.user = context.Authenticator().AuthenticateUser(userName, password)
		if h.user == nil {
			base.Logf("HTTP auth failed for username=%q", userName)
			h.response.Header().Set("WWW-Authenticate", `Basic realm="Couchbase Sync Gateway"`)
			return base.HTTPErrorf(http.StatusUnauthorized, "Invalid login")
		}
		return nil
	}

	// Check cookie
	h.user, err = context.Authenticator().AuthenticateCookie(h.rq, h.response)
	if err != nil {
		return err
	} else if h.user != nil {
		return nil
	}

	// No auth given -- check guest access
	if h.user, err = context.Authenticator().GetUser(""); err != nil {
		return err
	}
	if h.privs == regularPrivs && h.user.Disabled() {
		h.response.Header().Set("WWW-Authenticate", `Basic realm="Couchbase Sync Gateway"`)
		return base.HTTPErrorf(http.StatusUnauthorized, "Login required")
	}

	return nil
}

func (h *handler) assertAdminOnly() {
	if h.privs != adminPrivs {
		panic("Admin-only handler called without admin privileges, on " + h.rq.RequestURI)
	}
}

func (h *handler) PathVar(name string) string {
	v := mux.Vars(h.rq)[name]

	//Escape special chars i.e. '+' otherwise they are removed by QueryUnescape()
	v = strings.Replace(v, "+", "%2B", -1)

	// Before routing the URL we explicitly disabled expansion of %-escapes in the path
	// (see function FixQuotedSlashes). So we have to unescape them now.
	v, _ = url.QueryUnescape(v)
	return v
}

func (h *handler) SetPathVar(name string, value string) {
	mux.Vars(h.rq)[name] = url.QueryEscape(value)
}

func (h *handler) getQuery(query string) string {
	return h.rq.URL.Query().Get(query)
}

func (h *handler) getJSONStringQuery(query string) string {
	var jsonString string
	rawString := h.getQuery(query)
	err := json.Unmarshal([]byte(rawString), &jsonString)
	if err != nil {
		return rawString
	} else {
		return jsonString
	}
}

func (h *handler) getBoolQuery(query string) bool {
	return h.getOptBoolQuery(query, false)
}

func (h *handler) getOptBoolQuery(query string, defaultValue bool) bool {
	q := h.getQuery(query)
	if q == "" {
		return defaultValue
	}
	return q == "true"
}

// Returns the integer value of a URL query, defaulting to 0 if unparseable
func (h *handler) getIntQuery(query string, defaultValue uint64) (value uint64) {
	return getRestrictedIntQuery(h.rq.URL.Query(), query, defaultValue, 0, 0, false)
}

func (h *handler) getJSONQuery(query string) (value interface{}, err error) {
	valueJSON := h.getQuery(query)
	if valueJSON != "" {
		err = json.Unmarshal([]byte(valueJSON), &value)
	}
	return
}

func (h *handler) getJSONStringArrayQuery(param string) ([]string, error) {
	var strings []string
	value := h.getQuery(param)
	if value != "" {
		if err := json.Unmarshal([]byte(value), &strings); err != nil {
			return nil, base.HTTPErrorf(http.StatusBadRequest, "%s URL param is not a JSON string array", param)
		}
	}
	return strings, nil
}

func (h *handler) userAgentIs(agent string) bool {
	userAgent := h.rq.Header.Get("User-Agent")
	return len(userAgent) > len(agent) && userAgent[len(agent)] == '/' && strings.HasPrefix(userAgent, agent)
}

// Returns the request body as a raw byte array.
func (h *handler) readBody() ([]byte, error) {
	return ioutil.ReadAll(h.requestBody)
}

// Parses a JSON request body, returning it as a Body map.
func (h *handler) readJSON() (db.Body, error) {
	var body db.Body
	return body, h.readJSONInto(&body)
}

// Parses a JSON request body into a custom structure.
func (h *handler) readJSONInto(into interface{}) error {
	return db.ReadJSONFromMIME(h.rq.Header, h.requestBody, into)
}

// Reads & parses the request body, handling either JSON or multipart.
func (h *handler) readDocument() (db.Body, error) {
	contentType, attrs, _ := mime.ParseMediaType(h.rq.Header.Get("Content-Type"))
	switch contentType {
	case "", "application/json":
		return h.readJSON()
	case "multipart/related":
		if DebugMultipart {
			raw, err := h.readBody()
			if err != nil {
				return nil, err
			}
			reader := multipart.NewReader(bytes.NewReader(raw), attrs["boundary"])
			body, err := db.ReadMultipartDocument(reader)
			if err != nil {
				ioutil.WriteFile("GatewayPUT.mime", raw, 0600)
				base.Warn("Error reading MIME data: copied to file GatewayPUT.mime")
			}
			return body, err
		} else {
			reader := multipart.NewReader(h.requestBody, attrs["boundary"])
			return db.ReadMultipartDocument(reader)
		}
	default:
		return nil, base.HTTPErrorf(http.StatusUnsupportedMediaType, "Invalid content type %s", contentType)
	}
}

func (h *handler) requestAccepts(mimetype string) bool {
	accept := h.rq.Header.Get("Accept")
	return accept == "" || strings.Contains(accept, mimetype) || strings.Contains(accept, "*/*")
}

func (h *handler) getBasicAuth() (username string, password string) {
	auth := h.rq.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Basic ") {
		decoded, err := base64.StdEncoding.DecodeString(auth[6:])
		if err == nil {
			components := strings.SplitN(string(decoded), ":", 2)
			if len(components) == 2 {
				return components[0], components[1]
			}
		}
	}
	return
}

func (h *handler) getBearerToken() string {
	auth := h.rq.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		token := auth[7:]
		return token
	}
	return ""
}

func (h *handler) currentEffectiveUserName() string {
	var effectiveName string

	if h.privs == adminPrivs {
		effectiveName = " (as ADMIN)"
	} else if h.user != nil {
		if h.user.Name() != "" {
			effectiveName = fmt.Sprintf(" (as %s)", h.user.Name())
		} else {
			effectiveName = " (as GUEST)"
		}
	}

	return effectiveName
}

//////// RESPONSES:

func (h *handler) setHeader(name string, value string) {
	h.response.Header().Set(name, value)
}

func (h *handler) setStatus(status int, message string) {
	h.status = status
	h.statusMessage = message
}

func (h *handler) disableResponseCompression() {
	switch r := h.response.(type) {
	case *EncodedResponseWriter:
		r.disableCompression()
	}
}

// Writes an object to the response in JSON format.
// If status is nonzero, the header will be written with that status.
func (h *handler) writeJSONStatus(status int, value interface{}) {
	if !h.requestAccepts("application/json") {
		base.Warn("Client won't accept JSON, only %s", h.rq.Header.Get("Accept"))
		h.writeStatus(http.StatusNotAcceptable, "only application/json available")
		return
	}

	jsonOut, err := json.Marshal(value)
	if err != nil {
		base.Warn("Couldn't serialize JSON for %v : %s", value, err)
		h.writeStatus(http.StatusInternalServerError, "JSON serialization failed")
		return
	}
	if PrettyPrint {
		var buffer bytes.Buffer
		json.Indent(&buffer, jsonOut, "", "  ")
		jsonOut = append(buffer.Bytes(), '\n')
	}
	h.setHeader("Content-Type", "application/json")
	if h.rq.Method != "HEAD" {
		if len(jsonOut) < 1000 {
			h.disableResponseCompression()
		}
		h.setHeader("Content-Length", fmt.Sprintf("%d", len(jsonOut)))
		if status > 0 {
			h.response.WriteHeader(status)
			h.setStatus(status, "")
		}
		h.response.Write(jsonOut)
	} else if status > 0 {
		h.response.WriteHeader(status)
		h.setStatus(status, "")
	}
}

func (h *handler) writeJSON(value interface{}) {
	h.writeJSONStatus(http.StatusOK, value)
}

func (h *handler) addJSON(value interface{}) {
	encoder := json.NewEncoder(h.response)
	err := encoder.Encode(value)
	if err != nil {
		base.Warn("Couldn't serialize JSON for %v : %s", value, err)
		panic("JSON serialization failed")
	}
}

func (h *handler) writeMultipart(subtype string, callback func(*multipart.Writer) error) error {
	if !h.requestAccepts("multipart/") {
		return base.HTTPErrorf(http.StatusNotAcceptable, "Response is multipart")
	}

	// Get the output stream. Due to a CouchDB bug, if we're sending to it we need to buffer the
	// output in memory so we can trim the final bytes.
	var output io.Writer
	var buffer bytes.Buffer
	if h.userAgentIs("CouchDB") {
		output = &buffer
	} else {
		output = h.response
	}

	writer := multipart.NewWriter(output)
	h.setHeader("Content-Type",
		fmt.Sprintf("multipart/%s; boundary=%q", subtype, writer.Boundary()))

	err := callback(writer)
	writer.Close()

	if err == nil && output == &buffer {
		// Trim trailing newline; CouchDB is allergic to it:
		_, err = h.response.Write(bytes.TrimRight(buffer.Bytes(), "\r\n"))
	}
	return err
}

func (h *handler) flush() {
	switch r := h.response.(type) {
	case http.Flusher:
		r.Flush()
	}
}

// If the error parameter is non-nil, sets the response status code appropriately and
// writes a CouchDB-style JSON description to the body.
func (h *handler) writeError(err error) {
	if err != nil {
		err = auth.OIDCToHTTPError(err) // Map OIDC/OAuth2 errors to HTTP form
		status, message := base.ErrorAsHTTPStatus(err)
		h.writeStatus(status, message)
	}
}

// Writes the response status code, and if it's an error writes a JSON description to the body.
func (h *handler) writeStatus(status int, message string) {
	if status < 300 {
		h.response.WriteHeader(status)
		h.setStatus(status, message)
		return
	}
	// Got an error:
	var errorStr string
	switch status {
	case http.StatusNotFound:
		errorStr = "not_found"
	case http.StatusConflict:
		errorStr = "conflict"
	default:
		errorStr = http.StatusText(status)
		if errorStr == "" {
			errorStr = fmt.Sprintf("%d", status)
		}
	}

	h.disableResponseCompression()
	h.setHeader("Content-Type", "application/json")
	h.response.WriteHeader(status)
	h.setStatus(status, message)
	jsonOut, _ := json.Marshal(db.Body{"error": errorStr, "reason": message})
	h.response.Write(jsonOut)
}

var kRangeRegex = regexp.MustCompile("^bytes=(\\d+)?-(\\d+)?$")

// Detects and partially HTTP content range requests.
// If the request _can_ accept ranges, sets the "Accept-Ranges" response header to "bytes".
//
// If there is no Range: request header, or if its valid is invalid, returns a status of 200,
// meaning that the caller should return the entire response as usual.
//
// If there is a request range but it exceeds the contentLength, returns status 416. The caller
// should treat this as an error and abort, returning that HTTP status code.
//
// If there is a useable range, it returns status 206 and the start and end in Go slice terms, i.e.
// starting at 0 and with the end non-inclusive. It also adds a "Content-Range" response header.
// It is then the _caller's_ responsibility to set it as the response status code (by calling
// h.response.WriteHeader(status)), and then write the indicated subrange of the response data.
func (h *handler) handleRange(contentLength uint64) (status int, start uint64, end uint64) {
	status = http.StatusOK
	if h.rq.Method == "GET" || h.rq.Method == "HEAD" {
		h.setHeader("Accept-Ranges", "bytes")
		status, start, end = parseHTTPRangeHeader(h.rq.Header.Get("Range"), contentLength)
		if status == http.StatusPartialContent {
			h.setHeader("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, contentLength))
			h.setStatus(http.StatusPartialContent, "Partial Content")
			end += 1 // make end non-inclusive, as in Go slices
		}
	}
	return
}

// Given an HTTP "Range:" header value, parses it and returns the approppriate HTTP status code,
// and the numeric byte range if appropriate:
// * If the Range header is empty or syntactically invalid, it ignores it and returns status=200.
// * If the header is valid but exceeds the contentLength, it returns status=416 (Not Satisfiable).
// * Otherwise it returns status=206 and sets the start and end values in HTTP terms, i.e. with
//   the first byte numbered 0, and the end value inclusive (so the first 100 bytes are 0-99.)
func parseHTTPRangeHeader(rangeStr string, contentLength uint64) (status int, start uint64, end uint64) {
	// http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
	status = http.StatusOK
	if rangeStr == "" {
		return
	}
	match := kRangeRegex.FindStringSubmatch(rangeStr)
	if match == nil || (match[1] == "" && match[2] == "") {
		return
	}
	startStr, endStr := match[1], match[2]
	var err error

	start = 0
	if startStr != "" {
		// byte-range-spec
		if start, err = strconv.ParseUint(startStr, 10, 64); err != nil {
			start = math.MaxUint64 // string is all digits, so must just be too big for uint64
		}
	} else if endStr == "" {
		return // "-" is an invalid range spec
	}

	end = contentLength - 1
	if endStr != "" {
		if end, err = strconv.ParseUint(endStr, 10, 64); err != nil {
			end = math.MaxUint64 // string is all digits, so must just be too big for uint64
		}
		if startStr == "" {
			// suffix-range-spec ("-nnn" means the last nnn bytes)
			if end == 0 {
				return http.StatusRequestedRangeNotSatisfiable, 0, 0
			} else if contentLength == 0 {
				return
			} else if end > contentLength {
				end = contentLength
			}
			start = contentLength - end
			end = contentLength - 1
		} else {
			if end < start {
				return // invalid range
			}
			if end >= contentLength {
				end = contentLength - 1 // trim to end of content
			}
		}
	}
	if start >= contentLength {
		return http.StatusRequestedRangeNotSatisfiable, 0, 0
	} else if start == 0 && end == contentLength-1 {
		return // no-op
	}

	// OK, it's a subrange:
	status = http.StatusPartialContent
	return
}

// Returns the integer value of a URL query, restricted to a min and max value,
// but returning 0 if missing or unparseable.  If allowZero is true, values coming in
// as zero will stay zero, instead of being set to the minValue.
func getRestrictedIntQuery(values url.Values, query string, defaultValue, minValue, maxValue uint64, allowZero bool) uint64 {
	return getRestrictedIntFromString(
		values.Get(query),
		defaultValue,
		minValue,
		maxValue,
		allowZero,
	)
}

func getRestrictedIntFromString(rawValue string, defaultValue, minValue, maxValue uint64, allowZero bool) uint64 {
	var value *uint64
	if rawValue != "" {
		intValue, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			value = nil
		} else {
			value = &intValue
		}
	}

	return getRestrictedInt(
		value,
		defaultValue,
		minValue,
		maxValue,
		allowZero,
	)
}

func getRestrictedInt(rawValue *uint64, defaultValue, minValue, maxValue uint64, allowZero bool) uint64 {

	var value uint64

	// Only use the defaultValue if rawValue isn't specified.
	if rawValue == nil {
		value = defaultValue
	} else {
		value = *rawValue
	}

	// If value is zero and allowZero=true, leave value at zero rather than forcing it to the minimum value
	validZero := (value == 0 && allowZero)
	if value < minValue && !validZero {
		value = minValue
	}

	if value > maxValue && maxValue > 0 {
		value = maxValue
	}

	return value
}
