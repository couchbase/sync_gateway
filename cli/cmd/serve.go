package cmd

import (
	"github.com/couchbase/sync_gateway"
	"github.com/spf13/cobra"
	"fmt"
)

var (
	GoCBConnstr string
	CBUsername string
	CBPassword string
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Sync Gateway Server",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {

		config, err := sync_gateway.NewGatewayBootstrapConfig(GoCBConnstr)
		if err != nil {
			panic(fmt.Sprintf("Error creating bootstrap config: %v", err))
		}

		config.CBUsername = CBUsername
		config.CBPassword = CBPassword

		sync_gateway.RunGateway(*config,true)
	},
}

func init() {

	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().StringVarP(&GoCBConnstr, "connstr", "c", "couchbase://host1,host2", "The Couchbase server(s) to connect to")

	serveCmd.Flags().StringVarP(&CBUsername, "username", "u", "username", "The Couchbase username to connect as")

	serveCmd.Flags().StringVarP(&CBPassword, "password", "p", "xxxxxxxx", "The password for the given username")


}
