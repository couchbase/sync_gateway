package cmd

import (
	"github.com/couchbase/sync_gateway"
	"github.com/spf13/cobra"
	"fmt"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Sync Gateway Server",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {

		config, err := sync_gateway.NewGatewayBootstrapConfig("http://localhost:9000,localhost:9001")
		if err != nil {
			panic(fmt.Sprintf("Error creating bootstrap config: %v", err))
		}

		sync_gateway.RunGateway(*config,true)
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// serveCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// serveCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
