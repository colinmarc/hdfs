package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(mkdirCmd)
	mkdirCmd.PersistentFlags().BoolVarP(&mkdirParentsOpt, "parents", "p", false, "no error if existing, make parent directories")
}

var (
	mkdirParentsOpt bool
)

var mkdirCmd = &cobra.Command{
	Use:   "mkdir [-p] FILE...",
	Short: "make HDFS directories",
	RunE:  mkdirRun,
	DisableFlagsInUseLine: true,
}

func mkdirRun(cmd *cobra.Command, args []string) error {
	paths, nn, err := normalizePaths(args)
	if err != nil {
		return err
	}

	if len(paths) == 0 {
		return cmd.Usage()
	}

	client, err := getClient(nn)
	if err != nil {
		return err
	}

	for _, p := range paths {
		if hasGlob(p) {
			return &os.PathError{
				Op:   "mkdir",
				Path: p,
				Err:  os.ErrNotExist,
			}
		}

		var mode = 0755 | os.ModeDir
		if mkdirParentsOpt {
			err = client.MkdirAll(p, mode)
		} else {
			err = client.Mkdir(p, mode)
		}

		if err != nil && !(mkdirParentsOpt && os.IsExist(err)) {
			return err
		}
	}
	return nil
}
