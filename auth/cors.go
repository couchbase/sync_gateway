package auth

import (
	"net/http"
	"strings"
)

// Configuration for Cross-Origin Resource Sharing
// <https://en.wikipedia.org/wiki/Cross-origin_resource_sharing>
type CORSConfig struct {
	Origin      []string `json:"origin,omitempty"       help:"List of allowed origins, use ['*'] to allow access from everywhere"`
	LoginOrigin []string `json:"login_origin,omitempty" help:"List of allowed login origins"`
	Headers     []string `json:"headers,omitempty"      help:"List of allowed headers"`
	MaxAge      int      `json:"max_age,omitempty"      help:"Maximum age of the CORS Options request"`
}

// Adds Access-Control-Allow-Origin, Access-Control-Allow-Credentials, Access-Control-Allow-Headers headers to an HTTP response.
func (cors *CORSConfig) AddResponseHeaders(request *http.Request, response http.ResponseWriter) {
	if originHeader := request.Header["Origin"]; len(originHeader) > 0 {
		origin := MatchedOrigin(cors.Origin, originHeader)
		response.Header().Add("Access-Control-Allow-Origin", origin)
		response.Header().Add("Access-Control-Allow-Credentials", "true")
		response.Header().Add("Access-Control-Allow-Headers", strings.Join(cors.Headers, ", "))
	}
}

func MatchedOrigin(allowOrigins []string, rqOrigins []string) string {
	for _, rv := range rqOrigins {
		for _, av := range allowOrigins {
			if rv == av {
				return av
			}
		}
	}
	for _, av := range allowOrigins {
		if av == "*" {
			return "*"
		}
	}
	return ""
}
