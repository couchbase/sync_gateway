package cmd

import (
	"context"
	"fmt"
	msgrpc "github.com/couchbase/mobile-service/mobile_service_grpc"
	"github.com/spf13/cobra"
)

var statusGatewaysCmd = &cobra.Command{
	Use:   "gateways",
	Short: "List all known gateway nodes",
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

		// 	ListSyncGateways(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*SyncGateways, error)
		gateways, err := adminClient.ListSyncGateways(context.Background(), &msgrpc.Empty{})
		if err != nil {
			panic(fmt.Sprintf("Error liting Sync Gateways: %v", err))
		}

		if len(gateways.Items) == 0 {
			fmt.Printf("No gateways found\n")
			return
		}
		for _, gateway := range gateways.Items {
			fmt.Printf("%s (%s)\n", gateway.SyncGatewayUUID, gateway.LastSeenTimestamp)
		}


	},
}

func init() {
	statusCmd.AddCommand(statusGatewaysCmd)
}

