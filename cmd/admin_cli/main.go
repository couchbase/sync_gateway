package main

const (
	DB1 = "db1"
	DB2 = "db2"
)

func main() {

	adminCli := NewAdminCLI()

	// adminCli.DeleteAllMobileKeys()

	adminCli.ListAllMobileKeys()

	// adminCli.UpsertServerConfig()

	adminCli.UpsertDbConfig(DB1)

	// adminCli.DeleteDbConfig(DB1)

}
