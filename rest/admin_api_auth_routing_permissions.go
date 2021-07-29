package rest

import (
	"fmt"
	"strings"
)

// Permission stores the name of a permission along whether it is database scoped. This is used to later obtain a
// formatted permission string for checking.
type Permission struct {
	PermissionName string
	DatabaseScoped bool
}

func (perm *Permission) FormattedName(bucketName string) string {
	if perm.DatabaseScoped {
		return fmt.Sprintf("cluster.bucket[%s]%s", bucketName, perm.PermissionName)
	}
	return fmt.Sprintf("cluster%s", perm.PermissionName)
}

func FormatPermissionNames(perms []Permission, bucketName string) (formattedPerms []string) {
	formattedPerms = make([]string, 0, len(perms))
	for _, perm := range perms {
		formattedPerms = append(formattedPerms, perm.FormattedName(bucketName))
	}
	return formattedPerms
}

func GetPermissionsNameFromFormatted(formattedName string) string {
	return "!" + strings.Split(formattedName, "!")[1]
}

func GetPermissionNameFromFormattedStrings(formattedNames []string) (perms []string) {
	perms = make([]string, 0, len(formattedNames))
	for _, formattedName := range formattedNames {
		perms = append(perms, GetPermissionsNameFromFormatted(formattedName))
	}
	return perms
}

// Permissions to use with admin handlers
var (
	PermCreateDb             = Permission{"!sgw_create_db", true}
	PermDeleteDb             = Permission{"!sgw_delete_db", true}
	PermUpdateDb             = Permission{"!sgw_update_db", true}
	PermConfigureSyncFn      = Permission{"!sgw_configure_sync_fn", true}
	PermConfigureAuth        = Permission{"!sgw_configure_auth", true}
	PermWritePrincipal       = Permission{"!sgw_write_principal", true}
	PermReadPrincipal        = Permission{"!sgw_read_principal", true}
	PermReadAppData          = Permission{"!sgw_read_appdata", true}
	PermReadPrincipalAppData = Permission{"!sgw_read_principal_appdata", true}
	PermWriteAppData         = Permission{"!sgw_write_appdata", true}
	PermWriteReplications    = Permission{"!sgw_write_replications", true}
	PermReadReplications     = Permission{"!sgw_read_replications", true}
	PermDevOps               = Permission{"!sgw_dev_ops", true}
	PermStatsExport          = Permission{"!stats_export", true}
)
