/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/kosalaat/file-replicator/pkg/client"
	"github.com/kosalaat/file-replicator/pkg/files"
	"github.com/spf13/cobra"
)

// senderCmd represents the sender command
var senderCmd = &cobra.Command{
	Use:   "sender",
	Short: "Configures the sender of the replication",
	Long: `Setup file-replicator as the sender (source data). For example:

file-replicator reciever --address localhost:50051 --file-root /path/to/monitor/ --block-size 8192 --parallelism 10
	`,
	Run: func(cmd *cobra.Command, args []string) {
		address, _ := cmd.Flags().GetString("address")
		fileRoot, _ := cmd.Flags().GetString("file-root")
		blockSize, _ := cmd.Flags().GetInt("block-size")
		parallelism, _ := cmd.Flags().GetInt("parallelism")

		replicationClient, err := client.NewReplicatorClient(address, fileRoot, uint64(parallelism))
		if err != nil {
			panic(fmt.Sprintf("Failed to create replication client: %v", err))
		}

		fileReplicator := &files.FileReplicator{
			ReplicatorClient: *replicationClient,
		}

		if err := fileReplicator.SetupFileWatcher(fileRoot, uint64(blockSize)); err != nil {
			panic(fmt.Sprintf("Failed to start monitoring: %v", err))
		}

		panic("got here")
	},
}

func init() {
	rootCmd.AddCommand(senderCmd)

	rootCmd.PersistentFlags().Int("block-size", 8192, "Size of the file blocks to be processed")
	rootCmd.PersistentFlags().Int("parallelism", 10, "Number of parallel file processing operations")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// senderCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// senderCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
