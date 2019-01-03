package cmd

import (
	"context"
	"fmt"

	msgrpc "github.com/couchbase/mobile-service/mobile_service_grpc"
	"github.com/spf13/cobra"
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export the SG config into JSON",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {

		bootstrapConfig, err := BootstrapConfigFromParams()
		if err != nil {
			panic(fmt.Sprintf("Error getting bootstrap config: %v", err))
		}

		// Set up a connection to the server.
		adminClient, err := bootstrapConfig.NewMobileServiceAdminClient()
		if err != nil {
			panic(fmt.Sprintf("Error connecting via grpc to mobile service: %v", err))
		}

		configJson, err := adminClient.ExportConfig(context.Background(), &msgrpc.Empty{})
		if err != nil {
			panic(fmt.Sprintf("Error getting Sync Gateway config: %v", err))
		}

		// Print the json body to stdout
		fmt.Printf("%s\n", configJson.Body)

	},
}

func init() {
	configCmd.AddCommand(exportCmd)
}
