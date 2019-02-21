//  Copyright (c) 2012-2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"expvar"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
	"github.com/couchbaselabs/gocbconnstr"
	"github.com/gorilla/mux"
	pkgerrors "github.com/pkg/errors"
)

const (
	kMaxDeltaTtl         = 60 * 60 * 24 * 30
	kMaxDeltaTtlDuration = 60 * 60 * 24 * 30 * time.Second
)

// basicAuthURLRegexp is used to match the HTTP basic auth component of a URL
var basicAuthURLRegexp = regexp.MustCompilePOSIX(`:\/\/[^:/]+:[^@/]+@`)

// RedactBasicAuthURL returns the given string, with a redacted HTTP basic auth component.
func RedactBasicAuthURL(url string) string {
	return basicAuthURLRegexp.ReplaceAllLiteralString(url, "://****:****@")
}

func GenerateRandomSecret() string {
	randomBytes := make([]byte, 20)
	n, err := io.ReadFull(rand.Reader, randomBytes)
	if n < len(randomBytes) || err != nil {
		panic("RNG failed, can't create password")
	}
	return fmt.Sprintf("%x", randomBytes)
}

// Returns a cryptographically-random 160-bit number encoded as a hex string.
func CreateUUID() string {
	bytes := make([]byte, 16)
	n, err := rand.Read(bytes)
	if n < 16 {
		Panicf(KeyAll, "Failed to generate random ID: %s", err)
	}
	return fmt.Sprintf("%x", bytes)
}

// This is a workaround for an incompatibility between Go's JSON marshaler and CouchDB.
// Go parses JSON numbers into float64 type, and then when it marshals float64 to JSON it uses
// scientific notation if the number is more than six digits long, even if it's an integer.
// However, CouchDB doesn't seem to like scientific notation and throws an exception.
// (See <https://issues.apache.org/jira/browse/COUCHDB-1670>)
// Thus, this function, which walks through a JSON-compatible object and converts float64 values
// to int64 when possible.
// NOTE: This function works on generic map[string]interface{}, but *not* on types based on it,
// like db.Body. Thus, db.Body has a special FixJSONNumbers method -- call that instead.
// TODO: In Go 1.1 we will be able to use a new option in the JSON parser that converts numbers
// to a special number type that preserves the exact formatting.
func FixJSONNumbers(value interface{}) interface{} {
	switch value := value.(type) {
	case float64:
		var asInt int64 = int64(value)
		if float64(asInt) == value {
			return asInt // Representable as int, so return it as such
		}
	case map[string]interface{}:
		for k, v := range value {
			value[k] = FixJSONNumbers(v)
		}
	case []interface{}:
		for i, v := range value {
			value[i] = FixJSONNumbers(v)
		}
	default:
	}
	return value
}

// Convert a JSON string, which has extra double quotes (eg, `"thing"`) into a normal string
// with the extra double quotes removed (eg "thing").  Normal strings will be returned as-is.
//
// `"thing"` -> "thing"
// "thing" -> "thing"
func ConvertJSONString(s string) string {
	var jsonString string
	err := json.Unmarshal([]byte(s), &jsonString)
	if err != nil {
		return s
	} else {
		return jsonString
	}
}

// Concatenates and merges multiple string arrays into one, discarding all duplicates (including
// duplicates within a single array.) Ordering is preserved.
func MergeStringArrays(arrays ...[]string) (merged []string) {
	seen := make(map[string]bool)
	for _, array := range arrays {
		for _, str := range array {
			if !seen[str] {
				seen[str] = true
				merged = append(merged, str)
			}
		}
	}
	return
}

func ToArrayOfInterface(arrayOfString []string) []interface{} {
	arrayOfInterface := make([]interface{}, len(arrayOfString))
	for i, v := range arrayOfString {
		arrayOfInterface[i] = v
	}
	return arrayOfInterface
}

func ToInt64(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case int64:
		return value, true
	case float64:
		return int64(value), true
	case int:
		return int64(value), true
	case json.Number:
		if n, err := value.Int64(); err == nil {
			return n, true
		}
	}
	return 0, false
}

func CouchbaseUrlWithAuth(serverUrl, username, password, bucketname string) (string, error) {

	// parse url and reconstruct it piece by piece
	u, err := url.Parse(serverUrl)
	if err != nil {
		return "", pkgerrors.WithStack(RedactErrorf("Error parsing serverUrl: %v.  Error: %v", MD(serverUrl), err))
	}

	userPass := bytes.Buffer{}
	addedUsername := false

	// do we have a username?  if so add it
	if username != "" {
		userPass.WriteString(username)
		addedUsername = true
	} else {
		// do we have a non-default bucket name?  if so, use that as the username
		if bucketname != "" && bucketname != "default" {
			userPass.WriteString(bucketname)
			addedUsername = true
		}
	}

	if addedUsername {
		if password != "" {
			userPass.WriteString(":")
			userPass.WriteString(password)
		}

	}

	if addedUsername {
		return fmt.Sprintf(
			"%v://%v@%v%v",
			u.Scheme,
			userPass.String(),
			u.Host,
			u.Path,
		), nil
	} else {
		// just return the original
		return serverUrl, nil
	}

}

// Callback function to return the stable sequence
type StableSequenceFunc func() (clock SequenceClock, err error)

// This transforms raw input bucket credentials (for example, from config), to input
// credentials expected by Couchbase server, based on a few rules
func TransformBucketCredentials(inputUsername, inputPassword, inputBucketname string) (username, password, bucketname string) {

	username = inputUsername
	password = inputPassword

	// If the username is empty then set the username to the bucketname.
	if inputUsername == "" {
		username = inputBucketname
	}

	// if the username is empty, then the password should be empty too
	if username == "" {
		password = ""
	}

	return username, password, inputBucketname

}

func IsPowerOfTwo(n uint16) bool {
	return (n & (n - 1)) == 0
}

// This is how Couchbase Server handles document expiration times
//
//The actual value sent may either be
//Unix time (number of seconds since January 1, 1970, as a 32-bit
//value), or a number of seconds starting from current time. In the
//latter case, this number of seconds may not exceed 60*60*24*30 (number
//of seconds in 30 days); if the number sent by a client is larger than
//that, the server will consider it to be real Unix time value rather
//than an offset from current time.
//
//This function takes a ttl as a Duration and returns an int
//formatted as required by CBS expiry processing
func DurationToCbsExpiry(ttl time.Duration) uint32 {
	if ttl <= kMaxDeltaTtlDuration {
		return uint32(ttl.Seconds())
	} else {
		return uint32(time.Now().Add(ttl).Unix())
	}
}

//This function takes a ttl in seconds and returns an int
//formatted as required by CBS expiry processing
func SecondsToCbsExpiry(ttl int) uint32 {
	return DurationToCbsExpiry(time.Duration(ttl) * time.Second)
}

//This function takes a CBS expiry and returns as a time
func CbsExpiryToTime(expiry uint32) time.Time {
	if expiry <= kMaxDeltaTtl {
		return time.Now().Add(time.Duration(expiry) * time.Second)
	} else {
		return time.Unix(int64(expiry), 0)
	}
}

// ReflectExpiry attempts to convert expiry from one of the following formats to a Couchbase Server expiry value:
//   1. Numeric JSON values are converted to uint32 and returned as-is
//   2. JSON numbers are converted to uint32 and returned as-is
//   3. String JSON values that are numbers are converted to int32 and returned as-is
//   4. String JSON values that are ISO-8601 dates are converted to UNIX time and returned
//   5. Null JSON values return 0
func ReflectExpiry(rawExpiry interface{}) (*uint32, error) {
	switch expiry := rawExpiry.(type) {
	case int64:
		return ValidateUint32Expiry(expiry)
	case float64:
		return ValidateUint32Expiry(int64(expiry))
	case json.Number:
		// Attempt to convert to int
		expInt, err := expiry.Int64()
		if err != nil {
			return nil, err
		}
		return ValidateUint32Expiry(expInt)
	case string:
		// First check if it's a numeric string
		expInt, err := strconv.ParseInt(expiry, 10, 32)
		if err == nil {
			return ValidateUint32Expiry(expInt)
		}
		// Check if it's an ISO-8601 date
		expRFC3339, err := time.Parse(time.RFC3339, expiry)
		if err == nil {
			return ValidateUint32Expiry(expRFC3339.Unix())
		} else {
			return nil, pkgerrors.Wrapf(err, "Unable to parse expiry %s as either numeric or date expiry", rawExpiry)
		}
	case nil:
		// Leave as zero/empty expiry
		return nil, nil
	default:
		return nil, fmt.Errorf("Unrecognized expiry format")
	}
}

func ValidateUint32Expiry(expiry int64) (*uint32, error) {
	if expiry < 0 || expiry > math.MaxUint32 {
		return nil, fmt.Errorf("Expiry value is not within valid range: %d", expiry)
	}
	uint32Expiry := uint32(expiry)
	return &uint32Expiry, nil
}

// Needed due to https://github.com/couchbase/sync_gateway/issues/1345
func AddDbPathToCookie(rq *http.Request, cookie *http.Cookie) {

	// "/db/foo" -> "db/foo"
	urlPathWithoutLeadingSlash := strings.TrimPrefix(rq.URL.Path, "/")

	dbPath := "/"
	pathComponents := strings.Split(urlPathWithoutLeadingSlash, "/")
	if len(pathComponents) > 0 && pathComponents[0] != "" {
		dbPath = fmt.Sprintf("/%v", pathComponents[0])
	}
	cookie.Path = dbPath

}

// A retry sleeper is called back by the retry loop and passed
// the current retryCount, and should return the amount of milliseconds
// that the retry should sleep.
type RetrySleeper func(retryCount int) (shouldContinue bool, timeTosleepMs int)

// A RetryWorker encapsulates the work being done in a Retry Loop.  The shouldRetry
// return value determines whether the worker will retry, regardless of the err value.
// If the worker has exceeded it's retry attempts, then it will not be called again
// even if it returns shouldRetry = true.
type RetryWorker func() (shouldRetry bool, err error, value interface{})

type TimeoutWorker func()

func RetryLoop(description string, worker RetryWorker, sleeper RetrySleeper) (error, interface{}) {

	numAttempts := 1

	for {
		shouldRetry, err, value := worker()
		if !shouldRetry {
			if err != nil {
				return err, nil
			}
			return nil, value
		}
		shouldContinue, sleepMs := sleeper(numAttempts)
		if !shouldContinue {
			if err == nil {
				err = fmt.Errorf("RetryLoop for %v giving up after %v attempts", description, numAttempts)
			}
			Warnf(KeyAll, "RetryLoop for %v giving up after %v attempts", description, numAttempts)
			return err, value
		}
		Debugf(KeyAll, "RetryLoop retrying %v after %v ms.", description, sleepMs)

		<-time.After(time.Millisecond * time.Duration(sleepMs))

		numAttempts += 1

	}
}

type WorkerResult struct {
	ShouldRetry bool
	Error       error
	Value       interface{}
}

func (w WorkerResult) Unwrap() (ShouldRetry bool, Error error, Value interface{}) {
	return w.ShouldRetry, w.Error, w.Value
}

func WrapRetryWorkerTimeout(worker RetryWorker) (timeoutWorker TimeoutWorker, resultChan chan WorkerResult) {

	resultChan = make(chan WorkerResult)

	timeoutWorker = func() {

		shouldRetry, err, value := worker()

		result := WorkerResult{
			ShouldRetry: shouldRetry,
			Error:       err,
			Value:       value,
		}
		resultChan <- result

	}

	return timeoutWorker, resultChan

}

func RetryLoopTimeout(description string, worker RetryWorker, sleeper RetrySleeper, timeoutPerInvocation time.Duration) (error, interface{}) {

	numAttempts := 1

	for {

		// Wrap the retry worker into a "timeout worker" function that can be run async and will write it's
		// result to a channel
		timeoutWorker, chWorkerResult := WrapRetryWorkerTimeout(worker)

		// Kick off the timeout worker in it's own goroutine
		go timeoutWorker()

		// Wait for either the timeout worker to send it's result on the channel, or for the timeout to expire
		select {

		case workerResult := <-chWorkerResult:
			shouldRetry, err, value := workerResult.Unwrap()

			if !shouldRetry {
				if err != nil {
					return err, nil
				}
				return nil, value
			}
			shouldContinue, sleepMs := sleeper(numAttempts)
			if !shouldContinue {
				if err == nil {
					err = fmt.Errorf("RetryLoop for %v giving up after %v attempts", description, numAttempts)
				}
				Warnf(KeyAll, "RetryLoop for %v giving up after %v attempts", description, numAttempts)
				return err, value
			}
			Debugf(KeyAll, "RetryLoop retrying %v after %v ms.", description, sleepMs)

			<-time.After(time.Millisecond * time.Duration(sleepMs))

			numAttempts += 1

		case <-time.After(timeoutPerInvocation):
			return fmt.Errorf("Invocation timeout after waiting %v for worker to complete", timeoutPerInvocation), nil
		}

	}
}

// Create a RetrySleeper that will double the retry time on every iteration and
// use the given parameters.
// The longest wait time can be calculated with: initialTimeToSleepMs * 2^maxNumAttempts
// The total wait time can be calculated with: initialTimeToSleepMs * 2^maxNumAttempts+1
func CreateDoublingSleeperFunc(maxNumAttempts, initialTimeToSleepMs int) RetrySleeper {

	timeToSleepMs := initialTimeToSleepMs

	sleeper := func(numAttempts int) (bool, int) {
		if numAttempts > maxNumAttempts {
			return false, -1
		}
		if numAttempts > 1 {
			timeToSleepMs *= 2
		}
		return true, timeToSleepMs
	}
	return sleeper

}

// Create a sleeper function that sleeps up to maxNumAttempts, sleeping timeToSleepMs each attempt
func CreateSleeperFunc(maxNumAttempts, timeToSleepMs int) RetrySleeper {

	sleeper := func(numAttempts int) (bool, int) {
		if numAttempts > maxNumAttempts {
			return false, -1
		}
		return true, timeToSleepMs
	}
	return sleeper

}

// Create a RetrySleeper that will double the retry time on every iteration, with each sleep not exceeding maxSleepPerRetryMs.
func CreateMaxDoublingSleeperFunc(maxNumAttempts, initialTimeToSleepMs int, maxSleepPerRetryMs int) RetrySleeper {

	timeToSleepMs := initialTimeToSleepMs

	sleeper := func(numAttempts int) (bool, int) {
		if numAttempts > maxNumAttempts {
			return false, -1
		}
		if numAttempts > 1 {
			timeToSleepMs *= 2
			if timeToSleepMs > maxSleepPerRetryMs {
				timeToSleepMs = maxSleepPerRetryMs
			}
		}
		return true, timeToSleepMs
	}
	return sleeper

}

// SortedUint64Slice attaches the methods of sort.Interface to []uint64, sorting in increasing order.
type SortedUint64Slice []uint64

func (s SortedUint64Slice) Len() int           { return len(s) }
func (s SortedUint64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s SortedUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Sort is a convenience method.
func (s SortedUint64Slice) Sort() {
	sort.Sort(s)
}

func WriteHistogram(expvarMap *expvar.Map, since time.Time, prefix string) {
	WriteHistogramForDuration(expvarMap, time.Since(since), prefix)
}

func WriteHistogramForDuration(expvarMap *expvar.Map, duration time.Duration, prefix string) {

	if LogDebugEnabled(KeyAll) {
		var durationMs int
		if duration < 1*time.Second {
			durationMs = int(duration/(100*time.Millisecond)) * 100
		} else {
			durationMs = int(duration/(1000*time.Millisecond)) * 1000
		}
		expvarMap.Add(fmt.Sprintf("%s-%06dms", prefix, durationMs), 1)
	}
}

/*
 * Returns a URL formatted string which excludes the path, query and fragment
 * This is used by _replicate to split the single URL passed in a CouchDB style
 * request into a source URL and a database name as used in sg_replicate
 */
func SyncSourceFromURL(u *url.URL) string {
	var buf bytes.Buffer
	if u.Scheme != "" {
		buf.WriteString(u.Scheme)
		buf.WriteByte(':')
	}
	if u.Scheme != "" || u.Host != "" || u.User != nil {
		buf.WriteString("//")
		if ui := u.User; ui != nil {
			buf.WriteString(ui.String())
			buf.WriteByte('@')
		}
		if h := u.Host; h != "" {
			buf.WriteString(h)
		}
	}

	return buf.String()
}

//Convert string or array into a string array, otherwise return nil
func ValueToStringArray(value interface{}) []string {
	switch valueType := value.(type) {
	case string:
		return []string{valueType}
	case []string:
		return valueType
	case []interface{}:
		result := make([]string, 0, len(valueType))
		for _, item := range valueType {
			if str, ok := item.(string); ok {
				result = append(result, str)
			}
		}
		return result
	default:
		return nil
	}
}

// Validates path argument is a path to a writable file
func IsFilePathWritable(fp string) (bool, error) {
	//Get the containing directory for the file
	containingDir := filepath.Dir(fp)

	//Check that containing dir exists
	_, err := os.Stat(containingDir)
	if err != nil {
		return false, pkgerrors.WithStack(RedactErrorf("Error checking if %s is not writable.  Error: %v", UD(fp), err))
	}

	//Check that the filePath points to a file not a directory
	fi, err := os.Stat(fp)
	if err == nil || !os.IsNotExist(err) {
		Warnf(KeyAll, "filePath exists")
		if fi.Mode().IsDir() {
			return false, RedactErrorf("IsFilePathWritable() called but %s is a directory rather than a file", UD(fp))
		}
	}

	//Now validate that the logfile is writable
	file, err := os.OpenFile(fp, os.O_WRONLY, 0666)
	defer file.Close()
	if err == nil {
		return true, nil
	}
	if os.IsPermission(err) {
		return false, pkgerrors.WithStack(RedactErrorf("Error checking if %s is not writable.  Error: %v", UD(fp), err))
	}

	return true, nil
}

// SanitizeRequestURL will return a sanitised string of the URL by:
// - Tagging mux path variables.
// - Tagging query parameters.
// - Replacing sensitive data from the URL query string with ******.
// Have to use string replacement instead of writing directly to the Values URL object, as only the URL's raw query is mutable.
func SanitizeRequestURL(req *http.Request, cachedQueryValues *url.Values) string {

	// Populate a cached copy of query values if nothing is passed in.
	if cachedQueryValues == nil {
		v := req.URL.Query()
		cachedQueryValues = &v
	}

	urlString := sanitizeRequestURLQueryParams(req.URL.String(), *cachedQueryValues)

	if RedactSystemData || RedactMetadata || RedactUserData {
		tagQueryParams(*cachedQueryValues, &urlString)
		tagPathVars(req, &urlString)
	}

	return urlString
}

// redactedPathVars is a lookup map of path variables to redaction types.
var redactedPathVars = map[string]string{
	"docid":   "UD",
	"attach":  "UD",
	"name":    "UD",
	"channel": "UD",

	// MD redaction is not yet supported.
	// "db":        "MD",
	// "newdb":     "MD",
	// "ddoc":      "MD",
	// "view":      "MD",
	// "sessionid": "MD",
}

// tagPathVars will tag all redactble path variables in the urlString for the given request.
func tagPathVars(req *http.Request, urlString *string) {
	if urlString == nil || req == nil {
		return
	}

	str := *urlString
	pathVars := mux.Vars(req)

	for k, v := range pathVars {
		switch redactedPathVars[k] {
		case "UD":
			str = strings.Replace(str, "/"+v, "/"+UD(v).Redact(), 1)
		case "MD":
			str = strings.Replace(str, "/"+v, "/"+MD(v).Redact(), 1)
		case "SD":
			str = strings.Replace(str, "/"+v, "/"+SD(v).Redact(), 1)
		}
	}

	*urlString = str
}

// redactedQueryParams is a lookup map of query params to redaction types.
var redactedQueryParams = map[string]string{
	"channels": "UD", // updateChangesOptionsFromQuery, handleChanges
	"doc_ids":  "UD", // updateChangesOptionsFromQuery, handleChanges
	"startkey": "UD", // handleAllDocs
	"endkey":   "UD", // handleAllDocs

	// MD redaction is not yet supported.
	// "since":     "MD", // handleDumpChannel, updateChangesOptionsFromQuery, handleChanges
	// "rev":       "MD", // handleGetDoc, handlePutDoc, handleDeleteDoc, handleDelLocalDoc, handleGetAttachment, handlePutAttachment
	// "open_revs": "MD", // handleGetDoc
}

func tagQueryParams(values url.Values, urlString *string) {
	if urlString == nil || len(values) == 0 {
		return
	}

	str := *urlString
	str, _ = url.QueryUnescape(str)

	for k, vals := range values {
		// Query params can have more than one value (i.e: foo=bar&foo=buz)
		for _, v := range vals {
			switch redactedQueryParams[k] {
			case "UD":
				str = strings.Replace(str, fmt.Sprintf("%s=%s", k, v), fmt.Sprintf("%s=%s", k, UD(v).Redact()), 1)
			case "MD":
				str = strings.Replace(str, fmt.Sprintf("%s=%s", k, v), fmt.Sprintf("%s=%s", k, MD(v).Redact()), 1)
			case "SD":
				str = strings.Replace(str, fmt.Sprintf("%s=%s", k, v), fmt.Sprintf("%s=%s", k, SD(v).Redact()), 1)
			}
		}
	}

	*urlString = str
}

// sanitizeRequestURLQueryParams replaces sensitive data from the URL query string with ******.
func sanitizeRequestURLQueryParams(urlStr string, values url.Values) string {

	if urlStr == "" || len(values) == 0 {
		return urlStr
	}

	// Do a basic contains for the values we care about, to minimize performance impact on other requests.
	if strings.Contains(urlStr, "code=") || strings.Contains(urlStr, "token=") {
		// Iterate over the URL values looking for matches, and then do a string replacement of the found value
		// into urlString.  Need to unescapte the urlString, as the values returned by URL.Query() get unescaped.
		urlStr, _ = url.QueryUnescape(urlStr)
		for key, vals := range values {
			if key == "code" || strings.Contains(key, "token") {
				//In case there are multiple entries
				for _, val := range vals {
					urlStr = strings.Replace(urlStr, fmt.Sprintf("%s=%s", key, val), fmt.Sprintf("%s=******", key), -1)
				}
			}
		}
	}

	return urlStr
}

// Convert a map of vbno->seq high sequences (as returned by couchbasebucket.GetStatsVbSeqno()) to a SequenceClock
func HighSeqNosToSequenceClock(highSeqs map[uint16]uint64) (*SequenceClockImpl, error) {

	seqClock := NewSequenceClockImpl()
	for vbNo, vbSequence := range highSeqs {
		seqClock.SetSequence(vbNo, vbSequence)
	}

	return seqClock, nil

}

// Make sure that the index bucket and data bucket have correct sequence parity
// See https://github.com/couchbase/sync_gateway/issues/1133 for more details
func VerifyBucketSequenceParity(indexBucketStableClock SequenceClock, bucket Bucket) error {

	maxVbNo, err := bucket.GetMaxVbno()
	if err != nil {
		return err
	}
	_, highSeqnos, err := bucket.GetStatsVbSeqno(maxVbNo, false)
	if err != nil {
		return err
	}
	dataBucketClock, err := HighSeqNosToSequenceClock(highSeqnos)
	if err != nil {
		return err
	}

	// The index bucket stable clock should be before or equal to the data bucket clock,
	// otherwise it could indicate that the data bucket has been _reset_ to empty or to
	// a value, which would render the index bucket incorrect
	if !indexBucketStableClock.AllBefore(dataBucketClock) {
		return fmt.Errorf("IndexBucketStable clock is not AllBefore the data bucket clock")
	}

	return nil

}

func GetGoCBBucketFromBaseBucket(baseBucket Bucket) (bucket CouchbaseBucketGoCB, err error) {
	switch baseBucket := baseBucket.(type) {
	case *CouchbaseBucketGoCB:
		return *baseBucket, nil
	default:
		return CouchbaseBucketGoCB{}, RedactErrorf("baseBucket %v was not a CouchbaseBucketGoCB.  Was type: %T", MD(baseBucket), baseBucket)
	}
}

func BooleanPointer(booleanValue bool) *bool {
	return &booleanValue
}

func StringPointer(value string) *string {
	return &value
}

// Convert a Couchbase URI (eg, couchbase://host1,host2) to a list of HTTP URLs with ports (eg, ["http://host1:8091", "http://host2:8091"])
// Primary use case is for backwards compatibility with go-couchbase, cbdatasource, and CBGT. Supports secure URI's as well (couchbases://).
// Related CBGT ticket: https://issues.couchbase.com/browse/MB-25522
func CouchbaseURIToHttpURL(bucket Bucket, couchbaseUri string) (httpUrls []string, err error) {

	// If we're using a gocb bucket, use the bucket to retrieve the mgmt endpoints.  Note that incoming bucket may be CouchbaseBucketGoCB or *CouchbaseBucketGoCB.
	switch typedBucket := bucket.(type) {
	case *CouchbaseBucketGoCB:
		if typedBucket.IoRouter() != nil {
			mgmtEps := typedBucket.IoRouter().MgmtEps()
			return mgmtEps, nil
		}
	default:
		// No bucket-based handling, fall back to URI parsing

	}

	// First try to do a simple URL parse, which will only work for http:// and https:// urls where there
	// is a single host.  If that works, return the result
	singleHttpUrl := SingleHostCouchbaseURIToHttpURL(couchbaseUri)
	if len(singleHttpUrl) > 0 {
		return []string{singleHttpUrl}, nil
	}

	// Unable to do simple URL parse, try to parse into components w/ gocbconnstr
	connSpec, errParse := gocbconnstr.Parse(couchbaseUri)
	if errParse != nil {
		return httpUrls, pkgerrors.WithStack(RedactErrorf("Error parsing gocb connection string: %v.  Error: %v", MD(couchbaseUri), errParse))
	}

	for _, address := range connSpec.Addresses {

		// Determine port to use for management API
		port := gocbconnstr.DefaultHttpPort

		translatedScheme := "http"
		switch connSpec.Scheme {

		case "couchbase":
			fallthrough
		case "couchbases":
			return nil, RedactErrorf("couchbase:// and couchbases:// URI schemes can only be used with GoCB buckets.  Bucket: %+v", MD(bucket))
		case "https":
			translatedScheme = "https"
		}

		if address.Port > 0 {
			port = address.Port
		} else {
			// If gocbconnstr didn't return a port, and it was detected to be an HTTPS connection,
			// change the port to the secure port 18091
			if translatedScheme == "https" {
				port = 18091
			}
		}

		httpUrl := fmt.Sprintf("%s://%s:%d", translatedScheme, address.Host, port)
		httpUrls = append(httpUrls, httpUrl)

	}

	return httpUrls, nil

}

// Special case for couchbaseUri strings that contain a single host with http:// or https:// schemes,
// possibly containing embedded basic auth.  Needed since gocbconnstr.Parse() will remove embedded
// basic auth from URLS.
func SingleHostCouchbaseURIToHttpURL(couchbaseUri string) (httpUrl string) {
	result, parseUrlErr := couchbase.ParseURL(couchbaseUri)

	// If there was an error parsing, return an empty string
	if parseUrlErr != nil {
		return ""
	}

	// If the host contains a "," then it parsed http://host1,host2 into a url with "host1,host2" as the host, which
	// is not going to work.  Return an empty string
	if strings.Contains(result.Host, ",") {
		return ""
	}

	// The scheme was couchbase://, but this method only deals with non-couchbase schemes, so return empty slice
	if strings.Contains(result.Scheme, "couchbase") {
		return ""
	}

	// It made it past all checks.  Return a slice with a single string
	return result.String()

}

// Slice a string to be less than or equal to desiredSze
func StringPrefix(s string, desiredSize int) string {
	if len(s) <= desiredSize {
		return s
	}

	return s[:desiredSize]
}

// Retrieves a slice from a byte, but returns error (instead of panic) if range isn't contained by the slice
func SafeSlice(data []byte, from int, to int) ([]byte, error) {
	if from > len(data) || to > len(data) || from > to {
		return nil, fmt.Errorf("Invalid slice [%d:%d] of []byte with len %d", from, to, len(data))
	}
	return data[from:to], nil
}

// Returns string representation of an expvar, given map name and key name
func GetExpvarAsString(mapName string, name string) string {
	mapVar := expvar.Get(mapName)
	expvarMap, ok := mapVar.(*expvar.Map)
	if !ok {
		return ""
	}
	value := expvarMap.Get(name)
	if value != nil {
		return value.String()
	} else {
		return ""
	}
}

// Returns int representation of an expvar, given map name and key name
func GetExpvarAsInt(mapName string, name string) (int, error) {
	stringVal := GetExpvarAsString(mapName, name)
	if stringVal == "" {
		return 0, nil
	}
	return strconv.Atoi(stringVal)
}

// TODO: temporary workaround until https://issues.couchbase.com/browse/MB-27026 is implemented
func ExtractExpiryFromDCPMutation(rq *gomemcached.MCRequest) (expiry uint32) {
	if len(rq.Extras) < 24 {
		return 0
	}
	return binary.BigEndian.Uint32(rq.Extras[20:24])
}

func StringSliceContains(set []string, target string) bool {
	for _, val := range set {
		if val == target {
			return true
		}
	}
	return false
}

func Uint32Ptr(u uint32) *uint32 {
	return &u
}

func BoolPtr(b bool) *bool {
	return &b
}

func ConvertToEmptyInterfaceSlice(i interface{}) (result []interface{}, err error) {
	switch v := i.(type) {
	case []string:
		result = make([]interface{}, len(v))
		for index, value := range v {
			result[index] = value
		}
		return result, nil
	case []interface{}:
		return v, nil
	default:
		return nil, fmt.Errorf("Unexpected type passed to ConvertToEmptyInterfaceSlice: %T", i)
	}

}

func isMinimumVersion(major, minor, minMajor, minMinor uint64) bool {
	if major < minMajor {
		return false
	}

	if major == minMajor && minor < minMinor {
		return false
	}

	return true
}

func HexCasToUint64(cas string) uint64 {
	casBytes, err := hex.DecodeString(strings.TrimPrefix(cas, "0x"))
	if err != nil || len(casBytes) != 8 {
		// Invalid cas - return zero
		return 0
	}

	return binary.LittleEndian.Uint64(casBytes[0:8])
}

func Crc32cHash(input []byte) uint32 {
	// crc32.MakeTable already ensures singleton table creation, so shouldn't need to cache.
	table := crc32.MakeTable(crc32.Castagnoli)
	return crc32.Checksum(input, table)
}

func Crc32cHashString(input []byte) string {
	return fmt.Sprintf("0x%x", Crc32cHash(input))
}

func SplitHostPort(hostport string) (string, string, error) {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return "", "", err
	}

	// If this is an IPv6 address, we need to rewrap it in []
	if strings.Contains(host, ":") {
		host = fmt.Sprintf("[%s]", host)
	}

	return host, port, nil
}

var backquoteStringRegexp = regexp.MustCompile("`((?s).*?)[^\\\\]`")

// ConvertBackQuotedStrings sanitises a string containing `...`-delimited strings.
// - Converts the backquotes into double-quotes
// - Escapes literal backslashes, newlines or double-quotes with backslashes.
func ConvertBackQuotedStrings(data []byte) []byte {
	return backquoteStringRegexp.ReplaceAllFunc(data, func(b []byte) []byte {

		b = bytes.Replace(b, []byte(`\`), []byte(`\\`), -1)
		b = bytes.Replace(b, []byte("\r"), []byte(""), -1)
		b = bytes.Replace(b, []byte("\n"), []byte(`\n`), -1)
		b = bytes.Replace(b, []byte("\t"), []byte(`\t`), -1)
		b = bytes.Replace(b, []byte(`"`), []byte(`\"`), -1)

		// Replace the backquotes with double-quotes
		b[0] = '"'
		b[len(b)-1] = '"'
		return b
	})
}

// FindPrimaryAddr returns the primary outbound IP of this machine.
// This is the same as find_primary_addr in sgcollect_info.
func FindPrimaryAddr() (net.IP, error) {
	conn, err := net.Dial("udp", "8.8.8.8:56")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP, nil
}

// ReplaceAll returns a string with all of the given chars replaced by new
func ReplaceAll(s, chars, new string) string {
	for _, r := range chars {
		s = strings.Replace(s, string(r), new, -1)
	}
	return s
}

// Convert an int into an *expvar.Int
func ExpvarIntVal(val int) *expvar.Int {
	value := expvar.Int{}
	value.Set(int64(val))
	return &value
}

func ExpvarInt64Val(val int64) *expvar.Int {
	value := expvar.Int{}
	value.Set(val)
	return &value
}

func ExpvarUInt64Val(val uint64) *expvar.Int {
	value := expvar.Int{}
	if val > math.MaxInt64 {
		value.Set(math.MaxInt64) // lossy, but expvar doesn't provide an alternative
	} else {
		value.Set(int64(val))
	}
	return &value
}

// Convert a float into an *expvar.Float
func ExpvarFloatVal(val float64) *expvar.Float {
	value := expvar.Float{}
	value.Set(float64(val))
	return &value
}

// Convert an expvar.Var to an int64.  Return 0 if the expvar var is nil.
func ExpvarVar2Int(expvarVar expvar.Var) int64 {
	if expvarVar == nil {
		return 0
	}
	asInt, ok := expvarVar.(*expvar.Int)
	if !ok {
		Warnf(KeyAll, "ExpvarVar2Int could not convert %v to *expvar.Int", expvarVar)
		return 0
	}
	return asInt.Value()
}

func ContainsString(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
