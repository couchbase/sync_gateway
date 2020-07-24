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
	"context"
	"encoding/base64"
	"encoding/json"
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

const (
	minCompressibleJSONSize = 1000
)

// If set to true, JSON output will be pretty-printed.
var PrettyPrint bool = false

// If set to true, diagnostic data will be dumped if there's a problem with MIME multipart data
var DebugMultipart bool = false

var lastSerialNum uint64 = 0

func init() {
	DebugMultipart = (os.Getenv("GatewayDebugMultipart") != "")
}

var kNotFoundError = base.HTTPErrorf(http.StatusNotFound, "missing")
var kBadMethodError = base.HTTPErrorf(http.StatusMethodNotAllowed, "Method Not Allowed")
var kBadRequestError = base.HTTPErrorf(http.StatusMethodNotAllowed, "Bad Request")

// Encapsulates the state of handling an HTTP request.
type handler struct {
	server                *ServerContext
	rq                    *http.Request
	response              http.ResponseWriter
	status                int
	statusMessage         string
	requestBody           io.ReadCloser
	db                    *db.Database
	user                  auth.User
	privs                 handlerPrivs
	startTime             time.Time
	serialNumber          uint64
	formattedSerialNumber string
	loggedDuration        bool
	runOffline            bool
	queryValues           url.Values // Copy of results of rq.URL.Query()
}

type handlerPrivs int

const (
	regularPrivs = iota // Handler requires valid authentication
	publicPrivs         // Handler Handler checks auth and falls back to guest if invalid or missing
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

	var err error
	if h.server.config.CompressResponses == nil || *h.server.config.CompressResponses {
		if encoded := NewEncodedResponseWriter(h.response, h.rq); encoded != nil {
			h.response = encoded
			defer encoded.Close()
		}
	}

	var isRequestLogged bool
	defer func() {
		if !isRequestLogged {
			h.logRequestLine()
		}
	}()

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

	if base.EnableLogHTTPBodies {
		h.logRequestBody()
	}

	h.setHeader("Server", base.VersionString)

	// If there is a "db" path variable, look up the database context:
	var dbContext *db.DatabaseContext
	if dbname := h.PathVar("db"); dbname != "" {
		if dbContext, err = h.server.GetDatabase(dbname); err != nil {
			return err
		}
	}

	// If this call is in the context of a DB make sure the DB is in a valid state
	if dbContext != nil {
		if !h.runOffline {

			// get a read lock on the dbContext
			// When the lock is returned we know that the db state will not be changed by
			// any other call
			dbContext.AccessLock.RLock()

			// defer releasing the dbContext until after the handler method returns
			defer dbContext.AccessLock.RUnlock()

			dbState := atomic.LoadUint32(&dbContext.State)

			// if dbState == db.DBOnline, continue flow and invoke the handler method
			if dbState == db.DBOffline {
				// DB is offline, only handlers with runOffline true can run in this state
				return base.HTTPErrorf(http.StatusServiceUnavailable, "DB is currently under maintenance")
			} else if dbState != db.DBOnline {
				// DB is in transition state, no calls will be accepted until it is Online or Offline state
				return base.HTTPErrorf(http.StatusServiceUnavailable, fmt.Sprintf("DB is %v - try again later", db.RunStateString[dbState]))
			}
		}
	}

	// Authenticate, if not on admin port:
	if h.privs != adminPrivs {
		if err = h.checkAuth(dbContext); err != nil {
			return err
		}
	}

	h.logRequestLine()
	isRequestLogged = true

	// Now set the request's Database (i.e. context + user)
	if dbContext != nil {
		h.db, err = db.GetDatabase(dbContext, h.user)
		if err != nil {
			return err
		}
		h.db.Ctx = context.WithValue(context.Background(), base.LogContextKey{},
			base.LogContext{CorrelationID: h.formatSerialNumber()},
		)
	}

	if base.EnableLogHTTPBodies {
		// Wrap the existing ResponseWriter with one that "Tees" the output
		// to stdout as well as writing back to the socket
		h.response = NewLoggerTeeResponseWriter(
			h.response,
			base.KeyHTTP,
			h.formatSerialNumber(),
			h.rq,
			h.getQueryValues(),
		)
	}

	return method(h) // Call the actual handler code
}

func (h *handler) logRequestLine() {
	// Check Log Level first, as SanitizeRequestURL is expensive to evaluate.
	if !base.LogInfoEnabled(base.KeyHTTP) {
		return
	}

	proto := ""
	if h.rq.ProtoMajor >= 2 {
		proto = " HTTP/2"
	}

	queryValues := h.getQueryValues()
	base.Infof(base.KeyHTTP, " %s: %s %s%s%s", h.formatSerialNumber(), h.rq.Method, base.SanitizeRequestURL(h.rq, &queryValues), proto, h.formattedEffectiveUserName())
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
			base.KeyHTTP,
			h.formatSerialNumber(),
			h.rq,
			h.getQueryValues(),
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
	}

	// Log timings/status codes for errors under the HTTP log key
	// and the HTTPResp log key for everything else.
	logKey := base.KeyHTTPResp
	if h.status >= 300 {
		logKey = base.KeyHTTP
	}

	base.Infof(logKey, "%s:     --> %d %s  (%.1f ms)",
		h.formatSerialNumber(), h.status, h.statusMessage,
		float64(duration)/float64(time.Millisecond),
	)
}

// logStatusWithDuration will log the request status and the duration of the request.
func (h *handler) logStatusWithDuration(status int, message string) {
	h.setStatus(status, message)
	h.logDuration(true)
}

// logStatus will log the request status, but NOT the duration of the request.
// This is used for indefinitely-long handlers like _changes that we don't want to track duration of
func (h *handler) logStatus(status int, message string) {
	h.setStatus(status, message)
	h.logDuration(false) // don't track actual time
}

func (h *handler) checkAuth(context *db.DatabaseContext) (err error) {

	h.user = nil
	if context == nil {
		return nil
	}

	// Record Auth stats
	defer func(t time.Time) {
		delta := time.Since(t).Nanoseconds()
		context.DbStats.NewStats.Security().TotalAuthTime.Add(delta)
		if err != nil {
			context.DbStats.NewStats.Security().AuthFailedCount.Add(1)
		} else {
			context.DbStats.NewStats.Security().AuthSuccessCount.Add(1)
		}

	}(time.Now())

	// If oidc enabled, check for bearer ID token
	if context.Options.OIDCOptions != nil {
		if token := h.getBearerToken(); token != "" {
			var authJwtErr error
			h.user, authJwtErr = context.Authenticator().AuthenticateUntrustedJWT(token, context.OIDCProviders, h.getOIDCCallbackURL)
			if h.user == nil || authJwtErr != nil {
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
		if context.Options.UnsupportedOptions.OidcTestProvider.Enabled && strings.HasSuffix(h.rq.URL.Path, "/_oidc_testing/token") {
			if username, password := h.getBasicAuth(); username != "" && password != "" {
				provider := context.Options.OIDCOptions.Providers.GetProviderForIssuer(issuerUrlForDB(h, context.Name), testProviderAudiences)
				if provider != nil && provider.ValidationKey != nil {
					if provider.ClientID == username && *provider.ValidationKey == password {
						return nil
					}
				}
			}
		}
	}

	// Check basic auth first
	if userName, password := h.getBasicAuth(); userName != "" {
		h.user = context.Authenticator().AuthenticateUser(userName, password)
		if h.user == nil {
			base.Infof(base.KeyAll, "HTTP auth failed for username=%q", base.UD(userName))
			if context.Options.SendWWWAuthenticateHeader == nil || *context.Options.SendWWWAuthenticateHeader {
				h.response.Header().Set("WWW-Authenticate", `Basic realm="Couchbase Sync Gateway"`)
			}
			return base.HTTPErrorf(http.StatusUnauthorized, "Invalid login")
		}
		return nil
	}

	// Check cookie
	h.user, err = context.Authenticator().AuthenticateCookie(h.rq, h.response)
	if err != nil && h.privs != publicPrivs {
		return err
	} else if h.user != nil {
		return nil
	}

	// No auth given -- check guest access
	if h.user, err = context.Authenticator().GetUser(""); err != nil {
		return err
	}
	if h.privs == regularPrivs && h.user.Disabled() {
		if context.Options.SendWWWAuthenticateHeader == nil || *context.Options.SendWWWAuthenticateHeader {
			h.response.Header().Set("WWW-Authenticate", `Basic realm="Couchbase Sync Gateway"`)
		}
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

func (h *handler) getQueryValues() url.Values {
	if h.queryValues == nil {
		h.queryValues = h.rq.URL.Query()
	}
	return h.queryValues
}

func (h *handler) getQuery(query string) string {
	return h.getQueryValues().Get(query)
}

func (h *handler) getJSONStringQuery(query string) string {
	return base.ConvertJSONString(h.getQuery(query))
}

func (h *handler) getBoolQuery(query string) bool {
	result, _ := h.getOptBoolQuery(query, false)
	return result
}

func (h *handler) getOptBoolQuery(query string, defaultValue bool) (result, isSet bool) {
	q := h.getQuery(query)
	if q == "" {
		return defaultValue, false
	}
	return q == "true", true
}

// Returns the integer value of a URL query, defaulting to 0 if unparseable
func (h *handler) getIntQuery(query string, defaultValue uint64) (value uint64) {
	return base.GetRestrictedIntQuery(h.getQueryValues(), query, defaultValue, 0, 0, false)
}

func (h *handler) getJSONQuery(query string) (value interface{}, err error) {
	valueJSON := h.getQuery(query)
	if valueJSON != "" {
		err = base.JSONUnmarshal([]byte(valueJSON), &value)
	}
	return
}

func (h *handler) getJSONStringArrayQuery(param string) ([]string, error) {
	var strings []string
	value := h.getQuery(param)
	if value != "" {
		if err := base.JSONUnmarshal([]byte(value), &strings); err != nil {
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
	return ReadJSONFromMIME(h.rq.Header, h.requestBody, into)
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
			body, err := ReadMultipartDocument(reader)
			if err != nil {
				_ = ioutil.WriteFile("GatewayPUT.mime", raw, 0600)
				base.Warnf("Error reading MIME data: copied to file GatewayPUT.mime")
			}
			return body, err
		} else {
			reader := multipart.NewReader(h.requestBody, attrs["boundary"])
			return ReadMultipartDocument(reader)
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

// taggedEffectiveUserName returns the tagged effective name of the user for the request.
// e.g: '<ud>alice</ud>' or 'GUEST'
func (h *handler) taggedEffectiveUserName() string {
	if h.privs == adminPrivs {
		return "ADMIN"
	} else if h.user == nil {
		return ""
	}

	// Tag actual user names ahead of actual logging, as we lose the UD type information
	if name := h.user.Name(); name != "" {
		return base.UD(name).Redact()
	}

	return "GUEST"
}

// formattedEffectiveUserName formats an effective name for appending to logs.
// e.g: 'Did xyz (as %s)' or 'Did xyz (as <ud>alice</ud>)'
func (h *handler) formattedEffectiveUserName() string {
	if name := h.taggedEffectiveUserName(); name != "" {
		return " (as " + name + ")"
	}

	return ""
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

// Do not call from HTTP handlers. Use h.writeRawJSON/h.writeRawJSONStatus instead.
// writeRawJSONWithoutClientVerification takes the given bytes and always writes the response as JSON,
// without checking that the client can accept it.
func (h *handler) writeRawJSONWithoutClientVerification(status int, b []byte) {
	if h.rq.Method != "HEAD" {
		h.setHeader("Content-Type", "application/json")
		if len(b) < minCompressibleJSONSize {
			h.disableResponseCompression()
		}
		h.setHeader("Content-Length", fmt.Sprintf("%d", len(b)))
		if status > 0 {
			h.response.WriteHeader(status)
			h.setStatus(status, "")
		}
		_, _ = h.response.Write(b)
	} else if status > 0 {
		h.response.WriteHeader(status)
		h.setStatus(status, "")
	}
}

// writeJSON writes the given value as a JSON response with a 200 OK status.
func (h *handler) writeJSON(value interface{}) {
	h.writeJSONStatus(http.StatusOK, value)
}

// Writes an object to the response in JSON format.
// If status is nonzero, the header will be written with that status.
func (h *handler) writeJSONStatus(status int, value interface{}) {
	if !h.requestAccepts("application/json") {
		base.Warnf("Client won't accept JSON, only %s", h.rq.Header.Get("Accept"))
		h.writeStatus(http.StatusNotAcceptable, "only application/json available")
		return
	}

	jsonOut, err := base.JSONMarshalCanonical(value)
	if err != nil {
		base.Warnf("Couldn't serialize JSON for %v : %s", base.UD(value), err)
		h.writeStatus(http.StatusInternalServerError, "JSON serialization failed")
		return
	}
	if PrettyPrint {
		var buffer bytes.Buffer
		_ = json.Indent(&buffer, jsonOut, "", "  ")
		jsonOut = append(buffer.Bytes(), '\n')
	}

	h.writeRawJSONWithoutClientVerification(status, jsonOut)
}

// writeRawJSON writes the given bytes as a JSON response with a 200 OK status.
func (h *handler) writeRawJSON(b []byte) {
	h.writeRawJSONStatus(http.StatusOK, b)
}

// writeRawJSONStatus writes the given bytes as a JSON response.
// If status is nonzero, the header will be written with that status.
func (h *handler) writeRawJSONStatus(status int, b []byte) {
	if !h.requestAccepts("application/json") {
		base.Warnf("Client won't accept JSON, only %s", h.rq.Header.Get("Accept"))
		h.writeStatus(http.StatusNotAcceptable, "only application/json available")
		return
	}

	h.writeRawJSONWithoutClientVerification(status, b)
}

// writeRawJSON writes the given bytes as a plaintext response with a 200 OK status.
func (h *handler) writeText(value []byte) {
	h.writeTextStatus(http.StatusOK, value)
}

// writeTextStatus writes the given bytes as a plaintext response.
// If status is nonzero, the header will be written with that status.
func (h *handler) writeTextStatus(status int, value []byte) {
	if !h.requestAccepts("text/plain") {
		base.Warnf("Client won't accept text/plain, only %s", h.rq.Header.Get("Accept"))
		h.writeStatus(http.StatusNotAcceptable, "only text/plain available")
		return
	}

	h.setHeader("Content-Type", "text/plain charset=utf-8")
	h.setHeader("Content-Length", fmt.Sprintf("%d", len(value)))
	if status > 0 {
		h.response.WriteHeader(status)
		h.setStatus(status, "")
	}
	_, _ = h.response.Write(value)
}

func (h *handler) addJSON(value interface{}) error {
	encoder := base.JSONEncoderCanonical(h.response)
	err := encoder.Encode(value)
	if err != nil {
		brokenPipeError := strings.Contains(err.Error(), "write: broken pipe")
		connectionResetError := strings.Contains(err.Error(), "write: connection reset")
		if brokenPipeError || connectionResetError {
			base.Debugf(base.KeyCRUD, "Couldn't serialize document body, HTTP client closed connection")
			return err
		} else {
			base.Warnf("Couldn't serialize JSON for %s", err)
			h.writeStatus(http.StatusInternalServerError, "Couldn't serialize document body")
		}
	}
	return nil
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
	_ = writer.Close()

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
		status, message := base.ErrorAsHTTPStatus(err)
		h.writeStatus(status, message)
		if status >= 500 {
			// Log additional context when the handler has a database reference
			if h.db != nil {
				base.ErrorfCtx(h.db.Ctx, "%s: %v", h.formatSerialNumber(), err)
			} else {
				base.Errorf("%s: %v", h.formatSerialNumber(), err)
			}
		}
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

	_, _ = h.response.Write([]byte(`{"error":"` + errorStr + `","reason":` + base.ConvertToJSONString(message) + `}`))
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

// formatSerialNumber returns the formatted serial number
func (h *handler) formatSerialNumber() string {
	if h.formattedSerialNumber == "" {
		h.formattedSerialNumber = fmt.Sprintf("#%03d", h.serialNumber)
	}
	return h.formattedSerialNumber
}
