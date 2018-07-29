package cmd

import (
	"errors"
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(chmodCmd)
	chmodCmd.PersistentFlags().BoolVarP(&chmodRecursiveOpt, "recursive", "R", false, "change files and directories recursively")
}

var (
	chmodRecursiveOpt bool
)

var chmodCmd = &cobra.Command{
	Use:   "chmod [-R] OCTAL-MODE FILE...",
	Short: "change HDFS file mode bits",
	RunE:  chmodRun,
	DisableFlagsInUseLine: true,
}

func chmodRun(cmd *cobra.Command, args []string) error {
	if len(args) < 2 {
		return cmd.Usage()
	}

	mode, err := strconv.ParseUint(args[0], 8, 32)
	if err != nil {
		return errors.New("invalid octal mode: " + args[0])
	}

	expanded, client, err := getClientAndExpandedPaths(args[1:])
	if err != nil {
		return err
	}

	visit := func(p string, fi os.FileInfo, err error) error {
		err = client.Chmod(p, os.FileMode(mode))

		if err != nil {
			return err
		}
		return nil
	}

	for _, p := range expanded {
		if chmodRecursiveOpt {
			err = client.Walk(p, visit)
			if err != nil {
				return err
			}
		} else {
			info, err := client.Stat(p)
			if err != nil {
				return err
			}

			visit(p, info, nil)
		}
	}
	return nil
}
