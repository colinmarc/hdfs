package cmd

import (
	"os"
	"time"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(touchCmd)
	//	touchCmd.PersistentFlags().BoolVarP(&touchAccessTimeOpt, "access-time", "a", false, "change only access time")
	touchCmd.PersistentFlags().BoolVarP(&touchNoCreateOpt, "no-create", "c", false, "do not create any files")
}

var (
	// Not implemented yet
	//touchModTimeOpt    bool
	//touchAccessTimeOpt bool
	touchNoCreateOpt bool
)

var touchCmd = &cobra.Command{
	Use:   "touch [-c] FILE...",
	Short: "create HDFS files and modify their timestamps",
	RunE:  touchRun,
	DisableFlagsInUseLine: true,
}

func touchRun(cmd *cobra.Command, args []string) error {
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
				Err:  os.ErrNotExist}
		}

		_, err := client.Stat(p)
		exists := !os.IsNotExist(err)
		if (err != nil && exists) || (!exists && touchNoCreateOpt) {
			return err
		}

		if exists {
			now := time.Now()
			mtime := now
			atime := now

			err = client.Chtimes(p, mtime, atime)
		} else {
			err = client.CreateEmptyFile(p)
		}

		if err != nil {
			return err
		}
	}
	return nil
}
