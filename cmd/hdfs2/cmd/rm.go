package cmd

import (
	"errors"
	"os"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(rmCmd)
	rmCmd.PersistentFlags().BoolVarP(&rmRecursiveOpt, "recursive", "r", false, "remove directories and their contents recursively")
	rmCmd.PersistentFlags().BoolVarP(&rmForceOpt, "force", "f", false, "ignore nonexistent files, never prompt")
}

var (
	rmRecursiveOpt bool
	rmForceOpt     bool
)

var rmCmd = &cobra.Command{
	Use:   "rm [-rf] FILE...",
	Short: "remove HDFS files or directories",
	RunE:  rmRun,
	DisableFlagsInUseLine: true,
}

func rmRun(cmd *cobra.Command, args []string) error {
	expanded, client, err := getClientAndExpandedPaths(args)
	if err != nil {
		return err
	}

	for _, p := range expanded {
		info, err := client.Stat(p)
		if err != nil {
			if rmForceOpt && os.IsNotExist(err) {
				continue
			}

			if pathErr, ok := err.(*os.PathError); ok {
				pathErr.Op = "remove"
			}

			return err
		}

		if !rmRecursiveOpt && info.IsDir() {
			return &os.PathError{
				Op:   "remove",
				Path: p,
				Err:  errors.New("file is a directory"),
			}
		}

		err = client.Remove(p)
		if err != nil {
			return err
		}
	}
	return nil
}
