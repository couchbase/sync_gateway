package cmd

import (
	"fmt"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/spf13/cobra"
	"math/rand"
	"time"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Sync Gateway Server",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {

		config, err := rest.NewGatewayBootstrapConfig(GoCBConnstr)
		if err != nil {
			panic(fmt.Sprintf("Error creating bootstrap config: %v", err))
		}

		config.PortOffset = PortOffset
		config.CBUsername = CBUsername
		config.CBPassword = CBPassword
		if UUID != "" {
			config.Uuid = UUID
		} else {
			config.Uuid = fmt.Sprintf("%d", time.Now().Unix())
		}

		gw, err := rest.StartGateway(*config)
		defer gw.Close()
		if err != nil {
			panic(fmt.Sprintf("Error starting gateway: %v", err))
		}
		gw.Wait()

	},
}

func init() {

	rand.Seed(time.Now().Unix())

	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().StringVarP(&UUID, "uuid", "d", "4fg6hf", "A UUID to uniquely identify this gateway node")

	serveCmd.Flags().IntVarP(&PortOffset, "portoffset", "r", 0, "Use this port offset for listening ports.  For example if set to 2, then the public port will be modified from 4984 -> 4986")

}
