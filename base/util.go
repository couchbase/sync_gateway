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
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	kMaxDeltaTtl         = 60 * 60 * 24 * 30
	kMaxDeltaTtlDuration = 60 * 60 * 24 * 30 * time.Second
)

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
		LogPanic("Failed to generate random ID: %s", err)
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

var kBackquoteStringRegexp *regexp.Regexp

// Preprocesses a string containing `...`-delimited strings. Converts the backquotes into double-quotes,
// and escapes any literal backslashes, newlines or double-quotes within them with backslashes.
func ConvertBackQuotedStrings(data []byte) []byte {
	if kBackquoteStringRegexp == nil {
		kBackquoteStringRegexp = regexp.MustCompile("`((?s).*?)[^\\\\]`")
	}
	// Find backquote-delimited strings and replace them:
	return kBackquoteStringRegexp.ReplaceAllFunc(data, func(bytes []byte) []byte {
		str := string(bytes)
		// Remove \r  and Escape literal backslashes, newlines and double-quotes:
		str = strings.Replace(str, `\`, `\\`, -1)
		str = strings.Replace(str, "\r", "", -1)
		str = strings.Replace(str, "\n", `\n`, -1)
		str = strings.Replace(str, "\t", `\t`, -1)
		str = strings.Replace(str, `"`, `\"`, -1)
		bytes = []byte(str)
		// Replace the backquotes with double-quotes
		bytes[0] = '"'
		bytes[len(bytes)-1] = '"'
		return bytes
	})
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
		return "", err
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

	// If it's the default bucket, then set the username to "default"
	// workaround for https://github.com/couchbaselabs/cbgt/issues/32#issuecomment-136481228
	if inputBucketname == "" || inputBucketname == "default" {
		username = "default"
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
func DurationToCbsExpiry(ttl time.Duration) int {
	if ttl <= kMaxDeltaTtlDuration {
		return int(ttl.Seconds())
	} else {
		return int(time.Now().Add(ttl).Unix())
	}
}

//This function takes a ttl in seconds and returns an int
//formatted as required by CBS expiry processing
func SecondsToCbsExpiry(ttl int) int {
	return DurationToCbsExpiry(time.Duration(ttl) * time.Second)
}

//This function takes a CBS expiry and returns as a time
func CbsExpiryToTime(expiry uint32) time.Time {
	if expiry <= kMaxDeltaTtl {
		return time.Now().Add(time.Duration(expiry) * time.Second)
	} else {
		log.Printf("expiry for %v becomes %v", expiry, time.Unix(int64(expiry), 0))
		return time.Unix(int64(expiry), 0)
	}
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
			Warn("RetryLoop for %v giving up after %v attempts", description, numAttempts)
			return err, value
		}
		LogTo("Debug", "RetryLoop retrying %v after %v ms.", description, sleepMs)

		<-time.After(time.Millisecond * time.Duration(sleepMs))

		numAttempts += 1

	}
}

// Create a RetrySleeper that will double the retry time on every iteration and
// use the given parameters
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

// Uint64Slice attaches the methods of sort.Interface to []uint64, sorting in increasing order.
type Uint64Slice []uint64

func (s Uint64Slice) Len() int           { return len(s) }
func (s Uint64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Uint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Sort is a convenience method.
func (s Uint64Slice) Sort() {
	sort.Sort(s)
}

func WriteHistogram(expvarMap *expvar.Map, since time.Time, prefix string) {
	WriteHistogramForDuration(expvarMap, time.Since(since), prefix)
}

func WriteHistogramForDuration(expvarMap *expvar.Map, duration time.Duration, prefix string) {

	if LogEnabled("PerfStats") {
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
		return false, err
	}

	//Check that the filePath points to a file not a directory
	fi, err := os.Stat(fp)
	if err == nil || !os.IsNotExist(err) {
		Warn("filePath exists")
		if fi.Mode().IsDir() {
			err = errors.New("filePath is a directory")
			return false, err
		}
	}

	//Now validate that the logfile is writable
	file, err := os.OpenFile(fp, os.O_WRONLY, 0666)
	defer file.Close()
	if err == nil {
		return true, nil
	}
	if os.IsPermission(err) {
		return false, err
	}

	return true, nil
}

// Replaces sensitive data from the URL query string with ******.
// Have to use string replacement instead of writing directly to the Values URL object, as only the URL's raw query is mutable.
func SanitizeRequestURL(requestURL *url.URL) string {
	urlString := requestURL.String()
	// Do a basic contains for the values we care about, to minimize performance impact on other requests.
	if strings.Contains(urlString, "code=") || strings.Contains(urlString, "token=") {
		// Iterate over the URL values looking for matches, and then do a string replacement of the found value
		// into urlString.  Need to unescapte the urlString, as the values returned by URL.Query() get unescaped.
		urlString, _ = url.QueryUnescape(urlString)
		values := requestURL.Query()
		for key, vals := range values {
			if key == "code" || strings.Contains(key, "token") {
				//In case there are multiple entries
				for _, val := range vals {
					urlString = strings.Replace(urlString, fmt.Sprintf("%s=%s", key, val), fmt.Sprintf("%s=******", key), -1)
				}
			}
		}
	}

	return urlString
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
// https://github.com/couchbase/sync_gateway/issues/1133
func VerifyBucketSequenceParity(indexBucketStableClock SequenceClock, bucket Bucket) error {

	cbBucket, ok := bucket.(CouchbaseBucket)
	if !ok {
		Warn(fmt.Sprintf("Bucket is a %T not a base.CouchbaseBucket, skipping verifyBucketSequenceParity()\n", bucket))
		return nil
	}

	maxVbNo, err := cbBucket.GetMaxVbno()
	if err != nil {
		return err
	}
	_, highSeqnos, err := cbBucket.GetStatsVbSeqno(maxVbNo, false)
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
