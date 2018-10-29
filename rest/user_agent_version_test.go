package rest

import (
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
)

func TestUserAgentVersion(t *testing.T) {

	userAgentVersion := NewUserAgentVersion("")
	goassert.Equals(t, userAgentVersion.MajorVersion(), 0)
	goassert.Equals(t, userAgentVersion.MinorVersion(), 0)

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.0.0")
	goassert.Equals(t, userAgentVersion.MajorVersion(), 1)
	goassert.Equals(t, userAgentVersion.MinorVersion(), 0)

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.1.0")
	goassert.Equals(t, userAgentVersion.MajorVersion(), 1)
	goassert.Equals(t, userAgentVersion.MinorVersion(), 1)

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.2 (suff/goes/here)")
	goassert.Equals(t, userAgentVersion.MajorVersion(), 1)
	goassert.Equals(t, userAgentVersion.MinorVersion(), 2)

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.2(suff.goes.here)")
	goassert.Equals(t, userAgentVersion.MajorVersion(), 1)
	goassert.Equals(t, userAgentVersion.MinorVersion(), 2)

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.2 suff.goes/here)")
	goassert.Equals(t, userAgentVersion.MajorVersion(), 1)
	goassert.Equals(t, userAgentVersion.MinorVersion(), 2)

	userAgentVersion = NewUserAgentVersion("whatever/thing$$h3ysuz1!!///")
	goassert.Equals(t, userAgentVersion.MajorVersion(), 0)
	goassert.Equals(t, userAgentVersion.MinorVersion(), 0)

	userAgentVersion = NewUserAgentVersion("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9")
	goassert.Equals(t, userAgentVersion.MajorVersion(), 0)
	goassert.Equals(t, userAgentVersion.MinorVersion(), 0)

}

func TestUserAgentVersionIsVersionAfter(t *testing.T) {

	userAgentVersion := NewUserAgentVersion("CouchbaseLite/0.3 suff.goes/here)")
	goassert.True(t, userAgentVersion.IsBefore(1, 2))
	goassert.True(t, userAgentVersion.IsEqualToOrAfter(0, 2))

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.1.1 suff.goes/here)")
	goassert.True(t, userAgentVersion.IsBefore(1, 3))
	goassert.True(t, userAgentVersion.IsEqualToOrAfter(1, 0))

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.3 suff.goes/here)")
	goassert.True(t, userAgentVersion.IsEqualToOrAfter(1, 3))
	goassert.True(t, userAgentVersion.IsEqualToOrAfter(1, 2))
	goassert.True(t, userAgentVersion.IsBefore(1, 4))

}
