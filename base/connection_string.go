package base

import (
	"errors"
	"net"
	"regexp"
	"strconv"
	"strings"
)

// This file adapted from https://github.com/couchbase/tools-common/tree/69822d8835143927e78488b4d0d1f42357077f5d/connstr.

var (
	// ErrInvalidConnectionString is returned if the part matching regex results in a <nil> return value i.e. it doesn't
	// match against the input string.
	ErrInvalidConnectionString = errors.New("invalid connection string")

	// ErrNoAddressesParsed is returned when the resulting parsed connection string doesn't contain any addresses.
	ErrNoAddressesParsed = errors.New("parsed connection string contains no addresses")

	// ErrNoAddressesResolved is returned when the resolved connection string doesn't contain any addresses.
	ErrNoAddressesResolved = errors.New("resolved connection string contains no addresses")

	// ErrBadScheme is returned if the user supplied a scheme that's not supported. Currently 'http', 'https',
	// 'couchbase' and 'couchbases' are supported.
	ErrBadScheme = errors.New("bad scheme")

	// ErrBadPort is returned if the parsed port is an invalid 16 bit unsigned integer.
	ErrBadPort = errors.New("bad port")
)

const (
	// DefaultHTTPPort is the default http management port for Couchbase Server. Will be used when no port is supplied
	// when using a non-ssl scheme.
	DefaultHTTPPort = 8091

	// DefaultHTTPSPort is the default https management port for Couchbase Server. Will be used when no port is supplied
	// when using a ssl scheme.
	DefaultHTTPSPort = 18091
)

// Address represents an address used to connect to Couchbase Server. Should not be used directly i.e. access should be
// provided through a 'ConnectionString' or a 'ResolvedConnectionString'.
type Address struct {
	Host string
	Port uint16
}

// ConnectionString represents a connection string that can be supplied to the 'backup' tools to give the tools a node
// or nodes in a cluster to bootstrap from.
type ConnectionString struct {
	Scheme    string
	Addresses []Address
}

// ResolvedConnectionString is similar to a 'ConnectionString', however, addresses are resolved i.e. ports/schemes are
// converted into something that we can use to bootstrap from.
//
// NOTE: If provided with a valid srv record (an address with the scheme 'couchbase' or 'couchbases', and no port). This
// function will lookup the srv record and use those addresses.
type ResolvedConnectionString struct {
	UseSSL    bool
	Addresses []Address
}

var validSchemes = SetOf("", "http", "https", "couchbase", "couchbases")

// ParseConnectionString the given connection string and perform first tier validation i.e. it's possible for a parsed connection string
// to fail when the 'Resolve' function is called.
//
// For more information on the connection string formats accepted by this function, refer to the host formats
// documentation at https://docs.couchbase.com/server/7.0/backup-restore/cbbackupmgr-backup.html#host-formats.
func ParseConnectionString(connectionString string) (*ConnectionString, error) {
	// partMatcher matches and groups the different parts of a given connection string. For example:
	// couchbases://10.0.0.1:11222,10.0.0.2,10.0.0.3:11207
	// Group 1: couchbases://
	// Group 2: couchbases
	// Group 7: 10.0.0.1:11222,10.0.0.2,10.0.0.3:11207
	partMatcher := regexp.MustCompile(`((.*):\/\/)?(([^\/?:]*)(:([^\/?:@]*))?@)?([^\/?]*)(\/([^\?]*))?`)

	// hostMatcher matches and groups different parts of a comma separated list of hostname in a connection string.
	// For example:
	// 10.0.0.1:11222,10.0.0.2,10.0.0.3:11207
	// Match 1:
	//   Group 1: 10.0.0.1
	//   Group 3: 10.0.0.1
	//   Group 4: :11222
	//   Group 5: 11222
	// Match 2:
	//   Group 1: 10.0.0.2
	//   Group 3: 10.0.0.2
	//   Group 4:
	//   Group 5:
	// Match 3:
	//   Group 1: 10.0.0.3
	//   Group 3: 10.0.0.3
	//   Group 4: :11222
	//   Group 5: 11222
	hostMatcher := regexp.MustCompile(`((\[[^\]]+\]+)|([^;\,\:]+))(:([0-9]*))?(;\,)?`)

	parts := partMatcher.FindStringSubmatch(connectionString)
	if parts == nil {
		return nil, ErrInvalidConnectionString
	}

	parsed := &ConnectionString{
		Scheme: parts[2],
	}

	if !validSchemes.Contains(parsed.Scheme) {
		return nil, ErrBadScheme
	}

	// We don't need to check if 'FindAllStringSubmatch' returns <nil> since ranging over a <nil> slice results in no
	// iterations. We will then return an 'ErrNoAddressesParsed' error which is more informative than
	// 'ErrInvalidConnectionString'.
	for _, hostInfo := range hostMatcher.FindAllStringSubmatch(parts[7], -1) {
		address := Address{
			Host: hostInfo[1],
		}

		if hostInfo[5] != "" {
			port, err := strconv.ParseUint(hostInfo[5], 10, 16)
			if err != nil {
				return nil, ErrBadPort
			}

			address.Port = uint16(port)
		}

		parsed.Addresses = append(parsed.Addresses, address)
	}

	if len(parsed.Addresses) == 0 {
		return nil, ErrNoAddressesParsed
	}

	return parsed, nil
}

// Resolve the current connection string and return addresses which can be used to bootstrap from. Will perform
// additional validation, once resolved the connection string is valid and can be used.
func (c *ConnectionString) Resolve() (*ResolvedConnectionString, error) {
	var (
		defaultPort uint16
		resolved    = &ResolvedConnectionString{}
	)

	switch c.Scheme {
	case "http", "couchbase":
		defaultPort = DefaultHTTPPort
	case "https", "couchbases":
		defaultPort = DefaultHTTPSPort
		resolved.UseSSL = true
	case "":
		defaultPort = DefaultHTTPPort
	default:
		return nil, ErrBadScheme
	}

	if resolved := c.resolveSRV(); resolved != nil {
		return resolved, nil
	}

	for _, address := range c.Addresses {
		resolvedAddress := Address{
			Host: address.Host,
			Port: address.Port,
		}

		if address.Port == 0 || address.Port == defaultPort || address.Port == DefaultHTTPPort {
			resolvedAddress.Port = DefaultHTTPPort
			if resolved.UseSSL {
				resolvedAddress.Port = DefaultHTTPSPort
			}
		}

		resolved.Addresses = append(resolved.Addresses, resolvedAddress)
	}

	if len(resolved.Addresses) == 0 {
		return nil, ErrNoAddressesResolved
	}

	return resolved, nil
}

// resolveSRV attempts to resolve the connection string as an srv record, the resulting connection string will be
// non-nil if it was a valid srv record containing one or more addresses.
func (c *ConnectionString) resolveSRV() *ResolvedConnectionString {
	validScheme := func(scheme string) bool {
		return scheme == "couchbase" || scheme == "couchbases"
	}

	validHostnameNoIP := func(addresses []Address) bool {
		return len(addresses) == 1 && addresses[0].Port == 0
	}

	validIP := func(address string) bool {
		return strings.Contains(address, ":") || net.ParseIP(address) != nil
	}

	if !validScheme(c.Scheme) || !validHostnameNoIP(c.Addresses) || validIP(c.Addresses[0].Host) {
		return nil
	}

	_, servers, err := net.LookupSRV(c.Scheme, "tcp", c.Addresses[0].Host)
	if err != nil || len(servers) <= 0 {
		return nil
	}

	resolved := &ResolvedConnectionString{
		UseSSL: c.Scheme == "couchbases",
	}

	for _, server := range servers {
		resolved.Addresses = append(resolved.Addresses, Address{
			Host: strings.TrimSuffix(server.Target, "."),
			Port: srvPort(c.Scheme),
		})
	}

	return resolved
}

// srvPort - Returns the port which should be used when resolving an SRV record. We don't use the port from the record
// itself since by default, it points to the KV port.
func srvPort(scheme string) uint16 {
	if scheme == "couchbases" {
		return DefaultHTTPSPort
	}

	return DefaultHTTPPort
}
