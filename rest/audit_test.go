package rest

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

func TestAuditInjectableHeader(t *testing.T) {
	const headerName = "extra-audit-logging-header"
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Unsupported.AuditInfoProvider = &AuditInfoProviderConfig{
				RequestInfoHeaderName: base.StringPtr(headerName),
			}
		},
	})
	defer rt.Close()

}
