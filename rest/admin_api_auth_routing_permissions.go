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
		return fmt.Sprintf("cluster.collection[%s:_default:_default]%s", bucketName, perm.PermissionName)
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
	// Handles cases where we have a bucket / collection scope
	if split := strings.Split(formattedName, "]."); len(split) == 2 {
		return split[1]
	}

	// Handles the cluster scoped permissions
	if split := strings.Split(formattedName, "!"); len(split) == 2 {
		return split[1]
	}

	// Otherwise just return as there's not much else we can do
	return formattedName
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
	PermCreateDb             = Permission{".sgw.db!create", true}
	PermDeleteDb             = Permission{".sgw.db!delete", true}
	PermUpdateDb             = Permission{".sgw.db!update", true}
	PermConfigureSyncFn      = Permission{".sgw.sync_function!configure", true}
	PermConfigureAuth        = Permission{".sgw.auth!configure", true}
	PermWritePrincipal       = Permission{".sgw.principal!write", true}
	PermReadPrincipal        = Permission{".sgw.principal!read", true}
	PermReadAppData          = Permission{".sgw.appdata!read", true}
	PermReadPrincipalAppData = Permission{".sgw.principal_appdata!read", true}
	PermWriteAppData         = Permission{".sgw.appdata!write", true}
	PermWriteReplications    = Permission{".sgw.replications!write", true}
	PermReadReplications     = Permission{".sgw.replications!read", true}
	PermDevOps               = Permission{".sgw.dev_ops!all", false}
	PermStatsExport          = Permission{".admin.stats_export!read", false}
)
