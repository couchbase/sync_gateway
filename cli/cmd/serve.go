package cmd

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/couchbase/sync_gateway/rest"
	"github.com/spf13/cobra"
)

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

		// If the user passes in an explicit UUID on the CLI, then use it.
		if UUID != "" {
			config.Uuid = UUID
		} else {
			// Otherwise, make one up.
			// TODO: this should re-use the same UUID that CBGT uses, after CBGT is integrated
			config.Uuid = fmt.Sprintf("gw-%d", time.Now().Unix())
		}

		gw, err := rest.StartSyncGateway(*config)
		defer gw.Close()
		if err != nil {
			panic(fmt.Sprintf("Error starting gateway: %v", err))
		}
		gw.Wait() // blocks indefinitely

	},
}

func init() {

	rand.Seed(time.Now().Unix())

	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().StringVarP(
		&UUID,
		"uuid",
		"d",
		"",
		"A UUID to uniquely identify this gateway node",
	)

	serveCmd.Flags().IntVarP(
		&PortOffset,
		"portoffset",
		"r",
		0,
		"Offset that will be added to the port of configured listening ports under "+
			"metakv://mobile/gateway/config/listener.  Useful for running multiple SG's locally "+
			"with the identical config in MetaKV",
	)

}
