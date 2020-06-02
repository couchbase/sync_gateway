package auth

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/pkg/timeutil"
	"github.com/couchbase/sync_gateway/base"
	"github.com/jonboulle/clockwork"
)

const (
	MaxProviderConfigSyncInterval = 24 * time.Hour
	MinProviderConfigSyncInterval = time.Minute
)

var ErrChecksumNilHttpResponse = errors.New("can't calculate the checksum of nil response")

type providerConfigSyncer struct {
	config      *OIDCProvider
	minInterval time.Duration
	maxInterval time.Duration
	clock       clockwork.Clock
	checksum    string
}

func newProviderConfigSyncer() *providerConfigSyncer {
	return &providerConfigSyncer{
		clock:       clockwork.NewRealClock(),
		minInterval: MinProviderConfigSyncInterval,
		maxInterval: MaxProviderConfigSyncInterval,
	}
}

func (s *providerConfigSyncer) Run() chan struct{} {
	stop := make(chan struct{})
	var next pcsStepper
	next = &pcsStepNext{aft: time.Duration(0)}
	go func() {
		for {
			select {
			case <-s.clock.After(next.after()):
				next = next.step(s.sync)
			case <-stop:
				return
			}
		}
	}()
	return stop
}

func (s *providerConfigSyncer) sync() (time.Duration, error) {
	discoveryURL := strings.TrimSuffix(s.config.Issuer, "/") + OIDCDiscoveryConfigPath
	base.Debugf(base.KeyAuth, "Fetching custom provider config from %s", base.UD(discoveryURL))
	req, err := http.NewRequest(http.MethodGet, discoveryURL, nil)
	if err != nil {
		base.Debugf(base.KeyAuth, "Error building new request for URL %s: %v", base.UD(discoveryURL), err)
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		base.Debugf(base.KeyAuth, "Error invoking calling discovery URL %s: %v", base.UD(discoveryURL), err)
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unsuccessful response: %v", err)
	}

	var metadata ProviderMetadata
	if err := base.JSONDecoder(resp.Body).Decode(&metadata); err != nil {
		base.Debugf(base.KeyAuth, "Error parsing body %s: ", err)
		return 0, err
	}
	var ttl time.Duration
	var ok bool
	ttl, ok, err = cacheable(resp.Header)
	if err != nil {
		return 0, err
	} else if ok {
		metadata.ExpiresAt = s.clock.Now().UTC().Add(ttl)
	}
	if metadata.Issuer != s.config.Issuer {
		return 0, fmt.Errorf("oidc: issuer did not match the issuer returned by provider, expected %q got %q",
			s.config.Issuer, metadata.Issuer)
	}

	checksum, err := getChecksum(resp)
	if s.checksum == checksum {
		s.checksum = checksum
		base.Debugf(base.KeyAuth, "No change in discovery config detected at this time")
		return s.nextSyncAfter(metadata.ExpiresAt, s.clock), nil
	}
	s.checksum = checksum

	verifier := s.config.GenerateVerifier(&metadata, context.Background())
	s.config.client.SetVerifier(verifier)
	return s.nextSyncAfter(metadata.ExpiresAt, s.clock), nil
}

type pcsStepFunc func() (time.Duration, error)

type pcsStepper interface {
	after() time.Duration
	step(pcsStepFunc) pcsStepper
}

type pcsStepNext struct {
	aft time.Duration
}

func (n *pcsStepNext) after() time.Duration {
	return n.aft
}
func (n *pcsStepNext) step(fn pcsStepFunc) (next pcsStepper) {
	ttl, err := fn()
	if err == nil {
		next = &pcsStepNext{aft: ttl}
		base.Infof(base.KeyAuth, "Synced provider config, next attempt in %v", next.after())
	} else {
		next = &pcsStepRetry{aft: time.Second}
		base.Errorf("Provider config sync failed, retrying in %v: %v", next.after(), err)
	}
	return
}

type pcsStepRetry struct {
	aft time.Duration
}

func (r *pcsStepRetry) after() time.Duration {
	return r.aft
}

func (r *pcsStepRetry) step(fn pcsStepFunc) (next pcsStepper) {
	ttl, err := fn()
	if err == nil {
		next = &pcsStepNext{aft: ttl}
		base.Infof(base.KeyAuth, "Provider config sync no longer failing")
	} else {
		next = &pcsStepRetry{aft: timeutil.ExpBackoff(r.aft, time.Minute)}
		base.Errorf("Provider config sync still failing, retrying in %v: %v", next.after(), err)
	}
	return
}

func (s *providerConfigSyncer) nextSyncAfter(exp time.Time, clock clockwork.Clock) time.Duration {
	if exp.IsZero() {
		return s.maxInterval
	}
	t := exp.Sub(clock.Now()) / 2
	if t > s.maxInterval {
		t = s.maxInterval
	} else if t < s.minInterval {
		t = s.minInterval
	}
	return t
}

func cacheControlMaxAge(hdr string) (time.Duration, bool, error) {
	for _, field := range strings.Split(hdr, ",") {
		parts := strings.SplitN(strings.TrimSpace(field), "=", 2)
		k := strings.ToLower(strings.TrimSpace(parts[0]))
		if k != "max-age" {
			continue
		}

		if len(parts) == 1 {
			return 0, false, errors.New("max-age has no value")
		}

		v := strings.TrimSpace(parts[1])
		if v == "" {
			return 0, false, errors.New("max-age has empty value")
		}

		age, err := strconv.Atoi(v)
		if err != nil {
			return 0, false, err
		}

		if age <= 0 {
			return 0, false, nil
		}

		return time.Duration(age) * time.Second, true, nil
	}

	return 0, false, nil
}

func expires(date, expires string) (time.Duration, bool, error) {
	if date == "" || expires == "" {
		return 0, false, nil
	}

	te, err := time.Parse(time.RFC1123, expires)
	if err != nil {
		return 0, false, err
	}

	td, err := time.Parse(time.RFC1123, date)
	if err != nil {
		return 0, false, err
	}

	ttl := te.Sub(td)

	if ttl <= 0 {
		return 0, false, nil
	}

	return ttl, true, nil
}

func cacheable(hdr http.Header) (time.Duration, bool, error) {
	ttl, ok, err := cacheControlMaxAge(hdr.Get("Cache-Control"))
	if err != nil || ok {
		return ttl, ok, err
	}

	return expires(hdr.Get("Date"), hdr.Get("Expires"))
}

// getChecksum returns SHA1 checksum of the give HTTP response.
func getChecksum(response *http.Response) (checksum string, err error) {
	if response == nil {
		return "", ErrChecksumNilHttpResponse
	}
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	hash := sha1.New()
	hash.Write(bytes)
	return hex.EncodeToString(hash.Sum(nil)), nil
}
