package db

import (
	goassert "github.com/couchbaselabs/go.assert"
	"testing"
)

func TestWebhookString(t *testing.T) {
	var wh *Webhook

	wh = &Webhook{
		url: "http://username:password@example.com/foo",
	}
	goassert.Equals(t, wh.String(), "Webhook handler [http://****:****@example.com/foo]")

	wh = &Webhook{
		url: "http://example.com:9000/baz",
	}
	goassert.Equals(t, wh.String(), "Webhook handler [http://example.com:9000/baz]")
}

func TestSanitizedUrl(t *testing.T) {
	var wh *Webhook

	wh = &Webhook{
		url: "https://foo%40bar.baz:my-%24ecret-p%40%25%24w0rd@example.com:8888/bar",
	}
	goassert.Equals(t, wh.SanitizedUrl(), "https://****:****@example.com:8888/bar")

	wh = &Webhook{
		url: "https://example.com/does-not-count-as-url-embedded:basic-auth-credentials@qux",
	}
	goassert.Equals(t, wh.SanitizedUrl(), "https://example.com/does-not-count-as-url-embedded:basic-auth-credentials@qux")
}
