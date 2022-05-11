//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
	"github.com/gorilla/mux"
	pkgerrors "github.com/pkg/errors"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
)

const (
	kMaxDeltaTtl         = 60 * 60 * 24 * 30
	kMaxDeltaTtlDuration = 60 * 60 * 24 * 30 * time.Second
)

// RedactBasicAuthURLUserAndPassword returns the given string, with a redacted HTTP basic auth component.
func RedactBasicAuthURLUserAndPassword(urlIn string) string {
	redactedUrl, err := RedactBasicAuthURL(urlIn, false)
	if err != nil {
		WarnfCtx(context.Background(), "%v", err)
		return ""
	}
	return redactedUrl
}

// RedactBasicAuthURLPassword returns the given string, with a redacted HTTP basic auth password component.
func RedactBasicAuthURLPassword(urlIn string) string {
	redactedUrl, err := RedactBasicAuthURL(urlIn, true)
	if err != nil {
		WarnfCtx(context.Background(), "%v", err)
		return ""
	}
	return redactedUrl
}

func RedactBasicAuthURL(urlIn string, passwordOnly bool) (string, error) {
	urlParsed, err := url.Parse(urlIn)
	if err != nil {
		// err can't be wrapped or logged as it contains unredacted data from the provided url
		return "", fmt.Errorf("unable to redact URL, returning empty string")
	}
	if urlParsed.User != nil {
		user := urlParsed.User.Username()
		if !passwordOnly {
			user = RedactedStr
		}
		urlParsed.User = url.UserPassword(user, RedactedStr)
	}

	return urlParsed.String(), nil
}

// GenerateRandomSecret returns a cryptographically-secure 160-bit random number encoded as a hex string.
func GenerateRandomSecret() (string, error) {
	val, err := randCryptoHex(160)
	if err != nil {
		return "", fmt.Errorf("RNG failed, can't create password: %w", err)
	}
	return val, nil
}

// GenerateRandomID returns a cryptographically-secure 128-bit random number encoded as a hex string.
func GenerateRandomID() (string, error) {
	val, err := randCryptoHex(128)
	if err != nil {
		return "", fmt.Errorf("failed to generate random ID: %w", err)
	}
	return val, nil
}

// randCryptoHex returns a cryptographically-secure random number of length sizeBits encoded as a hex string.
func randCryptoHex(sizeBits int) (string, error) {
	if sizeBits%8 != 0 {
		return "", fmt.Errorf("randCryptoHex sizeBits must be a multiple of 8: %d", sizeBits)
	}

	b := make([]byte, sizeBits/8)

	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", b), nil
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
	err := JSONUnmarshal([]byte(s), &jsonString)
	if err != nil {
		return s
	} else {
		return jsonString
	}
}

// ConvertToJSONString takes a string, and returns a JSON string, with any illegal characters escaped.
func ConvertToJSONString(s string) string {
	b, _ := JSONMarshal(s)
	return string(b)
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

// DurationToCbsExpiry takes a ttl as a Duration and returns an int
// formatted as required by CBS expiry processing
func DurationToCbsExpiry(ttl time.Duration) uint32 {
	if ttl <= kMaxDeltaTtlDuration {
		return uint32(ttl.Seconds())
	} else {
		return uint32(time.Now().Add(ttl).Unix())
	}
}

// SecondsToCbsExpiry takes a ttl in seconds and returns an int
// formatted as required by CBS expiry processing
func SecondsToCbsExpiry(ttl int) uint32 {
	return DurationToCbsExpiry(time.Duration(ttl) * time.Second)
}

// CbsExpiryToTime takes a CBS expiry and returns as a time
func CbsExpiryToTime(expiry uint32) time.Time {
	if expiry <= kMaxDeltaTtl {
		return time.Now().Add(time.Duration(expiry) * time.Second)
	} else {
		return time.Unix(int64(expiry), 0)
	}
}

// CbsExpiryToDuration takes a CBS expiry and returns as a duration
func CbsExpiryToDuration(expiry uint32) time.Duration {
	if expiry <= kMaxDeltaTtl {
		return time.Duration(expiry) * time.Second
	} else {
		expiryTime := time.Unix(int64(expiry), 0)
		return time.Until(expiryTime)
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

type RetryCasWorker func() (shouldRetry bool, err error, value uint64)

type RetryTimeoutError struct {
	description string
	attempts    int
}

func NewRetryTimeoutError(description string, attempts int) *RetryTimeoutError {
	return &RetryTimeoutError{
		description: description,
		attempts:    attempts,
	}
}

func (r *RetryTimeoutError) Error() string {
	return fmt.Sprintf("RetryLoop for %v giving up after %v attempts", r.description, r.attempts)
}

func RetryLoop(description string, worker RetryWorker, sleeper RetrySleeper) (error, interface{}) {
	return RetryLoopCtx(description, worker, sleeper, context.Background())
}

func RetryLoopCtx(description string, worker RetryWorker, sleeper RetrySleeper, ctx context.Context) (error, interface{}) {

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
				err = NewRetryTimeoutError(description, numAttempts)
			}
			WarnfCtx(ctx, "RetryLoop for %v giving up after %v attempts", description, numAttempts)
			return err, value
		}
		DebugfCtx(ctx, KeyAll, "RetryLoop retrying %v after %v ms.", description, sleepMs)

		select {
		case <-ctx.Done():
			return fmt.Errorf("Retry loop for %v closed based on context", description), nil
		case <-time.After(time.Millisecond * time.Duration(sleepMs)):
		}

		numAttempts += 1

	}
}

// A version of RetryLoop that returns a strongly typed cas as uint64, to avoid interface conversion overhead for
// high throughput operations.
func RetryLoopCas(description string, worker RetryCasWorker, sleeper RetrySleeper) (error, uint64) {

	numAttempts := 1

	for {
		shouldRetry, err, value := worker()
		if !shouldRetry {
			if err != nil {
				return err, value
			}
			return nil, value
		}
		shouldContinue, sleepMs := sleeper(numAttempts)
		if !shouldContinue {
			if err == nil {
				err = NewRetryTimeoutError(description, numAttempts)
			}
			WarnfCtx(context.Background(), "RetryLoopCas for %v giving up after %v attempts", description, numAttempts)
			return err, value
		}
		DebugfCtx(context.Background(), KeyAll, "RetryLoopCas retrying %v after %v ms.", description, sleepMs)

		<-time.After(time.Millisecond * time.Duration(sleepMs))

		numAttempts += 1

	}
}

// SleeperFuncCtx wraps the given RetrySleeper with a context, so it can be cancelled, or have a deadline.
func SleeperFuncCtx(sleeperFunc RetrySleeper, ctx context.Context) RetrySleeper {
	return func(retryCount int) (bool, int) {
		if err := ctx.Err(); err != nil {
			return false, -1
		}
		return sleeperFunc(retryCount)
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

// CreateIndefiniteMaxDoublingSleeperFunc is similar to CreateMaxDoublingSleeperFunc, with the exception that there is no number of maximum retries.
func CreateIndefiniteMaxDoublingSleeperFunc(initialTimeToSleepMs int, maxSleepPerRetryMs int) RetrySleeper {
	timeToSleepMs := initialTimeToSleepMs

	sleeper := func(numAttempts int) (bool, int) {
		timeToSleepMs *= 2
		if timeToSleepMs > maxSleepPerRetryMs {
			timeToSleepMs = maxSleepPerRetryMs
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

// Convert string or array into a string array, otherwise return nil. If
// the input slice contains entries of mixed type, all string entries would
// be collected and returned as a slice and non-string entries as another.
func ValueToStringArray(value interface{}) ([]string, []interface{}) {
	var nonStrings []interface{}
	switch valueType := value.(type) {
	case string:
		return []string{valueType}, nil
	case []string:
		return valueType, nil
	case []interface{}:
		result := make([]string, 0, len(valueType))
		for _, item := range valueType {
			if str, ok := item.(string); ok {
				result = append(result, str)
			} else {
				nonStrings = append(nonStrings, item)
			}
		}
		return result, nonStrings
	default:
		nonStrings = append(nonStrings, valueType)
		return nil, nonStrings
	}
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

func GetGoCBBucketFromBaseBucket(baseBucket Bucket) (bucket CouchbaseBucketGoCB, err error) {
	switch baseBucket := baseBucket.(type) {
	case *CouchbaseBucketGoCB:
		return *baseBucket, nil
	default:
		return CouchbaseBucketGoCB{}, RedactErrorf("baseBucket %v was not a CouchbaseBucketGoCB.  Was type: %T", MD(baseBucket), baseBucket)
	}
}

// StdlibDurationPtr returns a pointer to the given time.Duration literal.
func StdlibDurationPtr(value time.Duration) *time.Duration {
	return &value
}

// DurationPtr returns a pointer to the given ConfigDuration literal.
func DurationPtr(value ConfigDuration) *ConfigDuration {
	return &value
}

// LogLevelPtr returns a pointer to the given LogLevel literal.
func LogLevelPtr(value LogLevel) *LogLevel {
	return &value
}

// StringPtr returns a pointer to the given string literal.
func StringPtr(value string) *string {
	return &value
}

// Uint16Ptr returns a pointer to the given uint16 literal.
func Uint16Ptr(u uint16) *uint16 {
	return &u
}

// Uint32Ptr returns a pointer to the given uint32 literal.
func Uint32Ptr(u uint32) *uint32 {
	return &u
}

// Uint64Ptr returns a pointer to the given uint64 literal.
func Uint64Ptr(u uint64) *uint64 {
	return &u
}

// UintPtr returns a pointer to the given uint literal.
func UintPtr(u uint) *uint {
	return &u
}

// IntPtr returns a pointer to the given int literal.
func IntPtr(i int) *int {
	return &i
}

// BoolPtr returns a pointer to the given bool literal.
func BoolPtr(b bool) *bool {
	return &b
}

// BoolDefault returns ifNil if b is nil, or else returns dereferenced value of b
func BoolDefault(b *bool, ifNil bool) bool {
	if b != nil {
		return *b
	}
	return ifNil
}

func Float32Ptr(f float32) *float32 {
	return &f
}

// Convert a Bucket, or a Couchbase URI (eg, couchbase://host1,host2) to a list of HTTP URLs with ports (eg, ["http://host1:8091", "http://host2:8091"])
// connSpec can be optionally passed in if available, to prevent unnecessary double-parsing of connstr
// Primary use case is for backwards compatibility with go-couchbase, cbdatasource, and CBGT. Supports secure URI's as well (couchbases://).
// Related CBGT ticket: https://issues.couchbase.com/browse/MB-25522
func CouchbaseURIToHttpURL(bucket Bucket, couchbaseUri string, connSpec *gocbconnstr.ConnSpec) (httpUrls []string, err error) {

	// If we're using a couchbase bucket, use the bucket to retrieve the mgmt endpoints.
	cbBucket, ok := AsCouchbaseStore(bucket)
	if ok {
		return cbBucket.MgmtEps()
	}

	// No bucket-based handling, fall back to URI parsing

	// First try to do a simple URL parse, which will only work for http:// and https:// urls where there
	// is a single host.  If that works, return the result
	singleHttpUrl := SingleHostCouchbaseURIToHttpURL(couchbaseUri)
	if len(singleHttpUrl) > 0 {
		return []string{singleHttpUrl}, nil
	}

	// Parse the given URI if we've not already got a connSpec
	if connSpec == nil {
		// Unable to do simple URL parse, try to parse into components w/ gocbconnstr
		newConnSpec, errParse := gocbconnstr.Parse(couchbaseUri)
		if errParse != nil {
			return httpUrls, pkgerrors.WithStack(RedactErrorf("Error parsing gocb connection string: %v.  Error: %v", MD(couchbaseUri), errParse))
		}
		connSpec = &newConnSpec
	}

	return connSpecToHTTPURLs(*connSpec)
}

func connSpecToHTTPURLs(connSpec gocbconnstr.ConnSpec) (httpUrls []string, err error) {

	for _, address := range connSpec.Addresses {

		// Determine port to use for management API
		port := gocbconnstr.DefaultHttpPort

		translatedScheme := "http"
		switch connSpec.Scheme {

		case "couchbase":
			fallthrough
		case "couchbases":
			return nil, RedactErrorf("couchbase:// and couchbases:// URI schemes can only be used with GoCB buckets.")
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

// Add auth credentials to the given urls, since CBGT cannot take auth handlers in certain API calls yet
func ServerUrlsWithAuth(urls []string, spec BucketSpec) (urlsWithAuth []string, err error) {
	urlsWithAuth = make([]string, len(urls))
	username, password, bucketName := spec.Auth.GetCredentials()
	for i, url := range urls {
		urlWithAuth, err := CouchbaseUrlWithAuth(
			url,
			username,
			password,
			bucketName,
		)
		if err != nil {
			return urlsWithAuth, err
		}
		urlsWithAuth[i] = urlWithAuth
	}
	return urlsWithAuth, nil
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

func encodeClusterVersion(major, minor int) int {
	return major*0x10000 + minor
}

func decodeClusterVersion(combined int) (major, minor int) {
	major = combined >> 16
	minor = combined - (major << 16)
	return major, minor
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

// Crc32cHashString returns a zero padded version of a crc32 hash to always be hexidecimal prefixed 8 character string
func Crc32cHashString(input []byte) string {
	return fmt.Sprintf("0x%08x", Crc32cHash(input))
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

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP, conn.Close()
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
		WarnfCtx(context.Background(), "ExpvarVar2Int could not convert %v to *expvar.Int", expvarVar)
		return 0
	}
	return asInt.Value()
}

// DefaultHTTPTransport returns a new HTTP Transport that copies values from http.DefaultTransport
func DefaultHTTPTransport() *http.Transport {
	// This type assertion will panic if http.DefaultTransport ever changes to not be a http.Transport
	// We'll catch this in development/unit testing pretty quickly if it does happen.
	return http.DefaultTransport.(*http.Transport).Clone()
}

// IsFleeceDeltaError returns true if the given error originates from go-fleecedelta.
func IsFleeceDeltaError(err error) bool { return errors.As(err, &FleeceDeltaError{}) }

// FleeceDeltaError is a typed error wrapped around any error returned from go-fleecedelta.
type FleeceDeltaError struct{ e error }

func (e FleeceDeltaError) Error() string { return e.e.Error() }
func (e FleeceDeltaError) Unwrap() error { return e.e }

func ContainsString(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// AtomicBool is a bool that can be set or read atomically
type AtomicBool struct {
	value int32
}

func NewAtomicBool(val bool) *AtomicBool {
	v := int32(0)
	if val {
		v = 1
	}
	return &AtomicBool{value: v}
}

func (ab *AtomicBool) Set(flag bool) {
	if flag {
		atomic.StoreInt32(&ab.value, 1)
	} else {
		atomic.StoreInt32(&ab.value, 0)
	}
}

func (ab *AtomicBool) IsTrue() bool {
	return atomic.LoadInt32(&ab.value) == 1
}

func (ab *AtomicBool) CompareAndSwap(old bool, new bool) bool {
	var oldint32 int32
	var newint32 int32
	if old {
		oldint32 = 1
	}
	if new {
		newint32 = 1
	}
	return atomic.CompareAndSwapInt32(&ab.value, oldint32, newint32)
}

// CASRetry attempts to retry CompareAndSwap for up to 1 second before returning the result.
func (ab *AtomicBool) CASRetry(old bool, new bool) bool {
	for i := 0; i < 100; i++ {
		if ab.CompareAndSwap(old, new) {
			return true
		}
		time.Sleep(time.Millisecond * 10)
	}
	return false
}

type AtomicInt struct {
	val int64
}

func (ai *AtomicInt) Set(value int64) {
	atomic.StoreInt64(&ai.val, value)
}

func (ai *AtomicInt) SetIfMax(value int64) {
	for {
		cur := atomic.LoadInt64(&ai.val)
		if cur >= value {
			return
		}

		if atomic.CompareAndSwapInt64(&ai.val, cur, value) {
			return
		}
	}
}

func (ai *AtomicInt) Add(value int64) {
	atomic.AddInt64(&ai.val, value)
}

func (ai *AtomicInt) Value() int64 {
	return atomic.LoadInt64(&ai.val)
}

type SafeTerminator struct {
	terminator       chan struct{}
	terminatorClosed AtomicBool
}

func NewSafeTerminator() *SafeTerminator {
	return &SafeTerminator{
		terminator: make(chan struct{}),
	}
}

func (t *SafeTerminator) IsClosed() bool {
	return t.terminatorClosed.IsTrue()
}

func (t *SafeTerminator) Done() <-chan struct{} {
	return t.terminator
}

func (t *SafeTerminator) Close() {
	if t.terminatorClosed.CompareAndSwap(false, true) {
		close(t.terminator)
	}
}

func Sha1HashString(str string, salt string) string {
	h := sha1.New()
	h.Write([]byte(salt + str))
	hashedKey := h.Sum(nil)
	return fmt.Sprintf("%x", hashedKey)
}

// KVPair represents a single KV pair to be used in InjectJSONProperties
type KVPair struct {
	Key string
	Val interface{}
}

// InjectJSONProperties takes the given JSON byte slice, and for each KV pair, marshals the value and inserts into
// the returned byte slice under the given key, without modifying the given byte slice.
//
// This has the potential to create duplicate keys, which whilst adhering to the spec, are ambiguous with how they get read...
// usually "last key wins" - although there is no standardized way of handling JSON with non-unique keys.
func InjectJSONProperties(b []byte, kvPairs ...KVPair) (new []byte, err error) {
	if len(kvPairs) == 0 {
		// noop
		return b, nil
	}

	b = bytes.TrimSpace(b)

	bIsJSONObject, bIsEmpty := isJSONObject(b)
	if !bIsJSONObject {
		return nil, errors.New("b is not a JSON object")
	}

	kvPairsBytes := make([]KVPairBytes, len(kvPairs))
	for i, kv := range kvPairs {
		var valBytes []byte
		var err error

		switch v := kv.Val.(type) {
		case int:
			valBytes = []byte(strconv.FormatInt(int64(v), 10))
		case int8:
			valBytes = []byte(strconv.FormatInt(int64(v), 10))
		case int16:
			valBytes = []byte(strconv.FormatInt(int64(v), 10))
		case int32:
			valBytes = []byte(strconv.FormatInt(int64(v), 10))
		case int64:
			valBytes = []byte(strconv.FormatInt(v, 10))
		case uint:
			valBytes = []byte(strconv.FormatUint(uint64(v), 10))
		case uint8:
			valBytes = []byte(strconv.FormatUint(uint64(v), 10))
		case uint16:
			valBytes = []byte(strconv.FormatUint(uint64(v), 10))
		case uint32:
			valBytes = []byte(strconv.FormatUint(uint64(v), 10))
		case uint64:
			valBytes = []byte(strconv.FormatUint(v, 10))
		case string:
			valBytes = []byte(`"` + v + `"`)
		case bool:
			valBytes = []byte(strconv.FormatBool(v))
		default:
			valBytes, err = JSONMarshal(kv.Val)
		}
		if err != nil {
			return nil, err
		}
		kvPairsBytes[i] = KVPairBytes{Key: kv.Key, Val: valBytes}
	}

	return injectJSONPropertyFromBytes(b, bIsEmpty, kvPairsBytes), nil
}

// KVPairBytes represents a single KV pair to be used in InjectJSONPropertiesFromBytes
type KVPairBytes struct {
	Key string
	Val []byte
}

// InjectJSONPropertiesFromBytes takes the given JSON byte slice, and for each KV pair, inserts into b under the given key.
//
// This has the potential to create duplicate keys, which whilst adhering to the spec, are ambiguous with how they get read...
// usually "last key wins" - although there is no standardized way of handling JSON with non-unique keys.
func InjectJSONPropertiesFromBytes(b []byte, kvPairs ...KVPairBytes) (new []byte, err error) {
	if len(kvPairs) == 0 {
		// noop
		return b, nil
	}

	b = bytes.TrimSpace(b)

	bIsJSONObject, bIsEmpty := isJSONObject(b)
	if !bIsJSONObject {
		return nil, errors.New("b is not a JSON object")
	}

	return injectJSONPropertyFromBytes(b, bIsEmpty, kvPairs), nil
}

// isJSONObject checks if the given bytes are a JSON object,
// and also whether it's an empty object or not.
func isJSONObject(b []byte) (isJSONObject, isEmpty bool) {

	// Check if the byte slice starts with { and ends with }
	if len(b) < 2 || b[0] != 0x7b || b[len(b)-1] != 0x7d {
		return false, false
	}

	return true, len(b) == 2
}

// injectJSONPropertyFromBytes injects val under the given key into b.
func injectJSONPropertyFromBytes(b []byte, bIsEmpty bool, kvPairs []KVPairBytes) (newJSON []byte) {

	newJSONLength := len(b)
	for _, kv := range kvPairs {
		newJSONLength += len(kv.Key) + len(kv.Val) + 4 // json overhead for comma, quoted key, and colon
	}
	if bIsEmpty {
		// Take off the length of a comma when the starting body is empty
		newJSONLength--
	}

	// Create the new byte slice with the required capacity
	newJSON = make([]byte, newJSONLength)

	// copy almost all of b, except the last closing brace
	offset := copy(newJSON, b[0:len(b)-1])

	for i, kv := range kvPairs {
		// if the body isn't empty, or we're not on our first value, prepend a comma before our property
		if i > 0 || !bIsEmpty {
			offset += copy(newJSON[offset:], ",")
		}

		// inject valBytes as the last property
		offset += copy(newJSON[offset:], `"`+kv.Key+`":`)
		offset += copy(newJSON[offset:], kv.Val)
	}

	// closing brace
	_ = copy(newJSON[offset:], "}")

	return newJSON
}

// WrapJSONUnknownFieldErr wraps JSON unknown field errors with ErrUnknownField for later checking via errors.Cause
func WrapJSONUnknownFieldErr(err error) error {
	if err != nil && strings.Contains(err.Error(), "unknown field") {
		return pkgerrors.WithMessage(ErrUnknownField, err.Error())
	}
	return err
}

// UseStdlibJSON if true, uses the stdlib JSON package.
// This variable is not thread-safe, and should be set only once on startup.
var UseStdlibJSON bool

// JSONIterError is returned by the JSON wrapper functions, whenever jsoniter returns a non-nil error.
type JSONIterError struct {
	E error
}

func (iterErr *JSONIterError) Error() string {
	return iterErr.E.Error()
}

// JSONDecoderI is the common interface between json.Decoder and jsoniter.Decoder
type JSONDecoderI interface {
	UseNumber()
	DisallowUnknownFields()
	Decode(v interface{}) error
	Buffered() io.Reader
	//Token() (json.Token, error) // Not implemented by jsoniter
	More() bool
}

// JSONEncoderI is the common interface between json.Encoder and jsoniter.Encoder
type JSONEncoderI interface {
	Encode(v interface{}) error
	SetIndent(prefix, indent string)
	SetEscapeHTML(on bool)
}

func FatalPanicHandler() {
	// Log any panics using the built-in loggers so that the stacktraces end up in SG log files before exiting.
	if r := recover(); r != nil {
		FatalfCtx(context.TODO(), "Unexpected panic: %v - stopping process\n%v", r, string(debug.Stack()))
	}
}

func MinInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func MaxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func MinUint64(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func MaxUint64(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}

func DiffUint32(x, y uint32) uint32 {
	if x > y {
		return x - y
	}
	return y - x
}

// GetRestrictedIntQuery returns the integer value of a URL query, restricted to a min and max value,
// but returning 0 if missing or unparseable.  If allowZero is true, values coming in
// as zero will stay zero, instead of being set to the minValue.
func GetRestrictedIntQuery(values url.Values, query string, defaultValue, minValue, maxValue uint64, allowZero bool) uint64 {
	return GetRestrictedIntFromString(
		values.Get(query),
		defaultValue,
		minValue,
		maxValue,
		allowZero,
	)
}

func GetRestrictedIntFromString(rawValue string, defaultValue, minValue, maxValue uint64, allowZero bool) uint64 {
	var value *uint64
	if rawValue != "" {
		intValue, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			value = nil
		} else {
			value = &intValue
		}
	}

	return GetRestrictedInt(
		value,
		defaultValue,
		minValue,
		maxValue,
		allowZero,
	)
}

func GetRestrictedInt(rawValue *uint64, defaultValue, minValue, maxValue uint64, allowZero bool) uint64 {

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

// GetHttpClient returns a new HTTP client with TLS certificate verification
// disabled when insecureSkipVerify is true and enabled otherwise.
func GetHttpClient(insecureSkipVerify bool) *http.Client {
	if insecureSkipVerify {
		transport := DefaultHTTPTransport()
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = new(tls.Config)
		}
		transport.TLSClientConfig.InsecureSkipVerify = true
		return &http.Client{Transport: transport}
	}
	return http.DefaultClient
}

// IsConnectionRefusedError returns true if the given error is due to a connection being actively refused.
func IsConnectionRefusedError(err error) bool {
	if err == nil {
		return false
	}

	var errorMessage string
	switch runtime.GOOS {
	case "linux", "darwin":
		errorMessage = "connection refused"
	case "windows":
		errorMessage = "target machine actively refused"
	}

	return strings.Contains(err.Error(), errorMessage)
}

// ConfigDuration is a time.Duration that supports JSON marshalling/unmarshalling.
// Underlying duration cannot be embedded due to requiring reflect access for mergo.
type ConfigDuration struct {
	D *time.Duration
}

func (d *ConfigDuration) Value() time.Duration {
	if d == nil || d.D == nil {
		return 0
	}
	return *d.D
}

// NewConfigDuration returns a *ConfigDuration from a time.Duration
func NewConfigDuration(d time.Duration) *ConfigDuration {
	return &ConfigDuration{D: &d}
}

func (d ConfigDuration) MarshalJSON() ([]byte, error) {
	duration := d.D
	if duration == nil {
		return nil, fmt.Errorf("cannot marshal nil duration")
	}
	return json.Marshal(duration.String())
}

func (d *ConfigDuration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := JSONUnmarshal(b, &v); err != nil {
		return err
	}

	val, ok := v.(string)
	if !ok {
		return fmt.Errorf("invalid duration type %T - must be string", v)
	}

	dur, err := time.ParseDuration(val)
	if err != nil {
		return err
	}

	*d = ConfigDuration{D: &dur}
	return nil
}

// TerminateAndWaitForClose will close the given terminator, and wait up to timeout for the done channel to be closed in response.
func TerminateAndWaitForClose(terminator chan struct{}, done chan struct{}, timeout time.Duration) error {
	if terminator == nil && done == nil {
		// noop
		return nil
	}

	if terminator == nil || done == nil {
		return errors.New("terminateAndWaitForClose requires both terminator and done channels")
	}

	close(terminator)
	t := time.NewTimer(timeout)
	select {
	case <-done:
		t.Stop()
	case <-t.C:
		return fmt.Errorf("terminateAndWaitForClose timed out waiting for done channel after %v", timeout)
	}

	return nil
}
