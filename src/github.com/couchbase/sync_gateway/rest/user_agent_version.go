package rest

import (
	"regexp"
	"strconv"

	"github.com/couchbase/sync_gateway/base"
)

type UserAgentVersion struct {
	requestHeader string
	majorVersion  int
	minorVersion  int
}

func NewUserAgentVersion(requestHeader string) *UserAgentVersion {
	userAgentVersion := UserAgentVersion{
		requestHeader: requestHeader,
	}
	userAgentVersion.parse()
	return &userAgentVersion
}

// Extract the major and minor version from the request header string
func (uav *UserAgentVersion) parse() {

	if uav.requestHeader == "" {
		return
	}

	// A word
	// A forward slash
	// A (captured) series of numbers -- major version
	// A dot (needs to be escaped)
	// A (captured) series of numbers -- minor version
	// A bunch of whatever (.*)
	regex := `CouchbaseLite/([0-9]*)\.([0-9]*).*`

	// Compile regex
	re1, err := regexp.Compile(regex)
	if err != nil {
		base.Warn("Error compiling regex: %v.  Err: %v", regex, err)
		return
	}

	// Find match and extract groups
	result := re1.FindStringSubmatch(uav.requestHeader)

	// We should have at least three things captured
	if len(result) < 3 {
		return
	}

	// The first result is the entire string that matched, ignore it
	_ = result[0]

	// Get the first group
	majorVersionStr := result[1]
	if majorVersionStr == "" {
		return
	}
	majorVersion, err := strconv.Atoi(majorVersionStr)
	if err != nil {
		return
	}
	uav.majorVersion = majorVersion

	// Get the second group
	minorVersionStr := result[2]
	if minorVersionStr == "" {
		return
	}
	minorVersion, err := strconv.Atoi(minorVersionStr)
	if err != nil {
		return
	}
	uav.minorVersion = minorVersion

}

func (uav UserAgentVersion) MajorVersion() int {
	return uav.majorVersion
}

func (uav UserAgentVersion) MinorVersion() int {
	return uav.minorVersion
}
