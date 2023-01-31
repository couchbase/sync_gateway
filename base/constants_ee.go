//go:build cb_sg_enterprise
// +build cb_sg_enterprise

package base

const (
	productEditionEnterprise = true
	productEditionShortName  = "EE"

	DefaultAutoImport = true // Whether Sync Gateway should auto-import docs, if not specified in the config
)
