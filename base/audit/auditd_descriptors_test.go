package audit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGenerateAuditdModuleDescriptor outputs a generated auditd module descriptor for sgAuditEvents.
func TestGenerateAuditdModuleDescriptor(t *testing.T) {
	b, err := generateAuditdModuleDescriptor(sgAuditEvents)
	require.NoError(t, err)
	t.Log(string(b))
}
