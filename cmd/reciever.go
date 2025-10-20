/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/kosalaat/file-replicator/pkg/server"
	"github.com/spf13/cobra"
)

// reciverCmd represents the reciver command
var recieverCmd = &cobra.Command{
	Use:   "reciever",
	Short: "Configures the reciever of the replication",
	Long: `Setup file-replicator as the reciever. For example:

file-replicator reciever --address localhost:50051 --file-root /path/to/receive/files
	`,
	Run: func(cmd *cobra.Command, args []string) {
		address, _ := cmd.Flags().GetString("address")
		fileRoot, _ := cmd.Flags().GetString("file-root")

		replicationServer := server.NewReplicationServer()

		if replicationServer.StartListening(address, fileRoot) != nil {
			panic("Failed to start the replication server")
		}
		defer replicationServer.StopListening()
	},
}

func init() {
	rootCmd.AddCommand(recieverCmd)

}
