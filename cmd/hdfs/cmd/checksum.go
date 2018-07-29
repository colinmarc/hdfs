package cmd

import (
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(checksumCmd)
}

var checksumCmd = &cobra.Command{
	Use:   "checksum FILE...",
	Short: "calculate the checksum of the HDFS files",
	RunE:  checksumRun,
	DisableFlagsInUseLine: true,
}

func checksumRun(cmd *cobra.Command, args []string) error {
	expanded, client, err := getClientAndExpandedPaths(args)
	if err != nil {
		return err
	}

	for _, p := range expanded {
		reader, err := client.Open(p)
		if err != nil {
			return err
		}

		checksum, err := reader.Checksum()
		if err != nil {
			return err
		}

		fmt.Println(hex.EncodeToString(checksum), p)
	}
	return nil
}
