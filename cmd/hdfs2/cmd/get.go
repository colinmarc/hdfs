package cmd

import (
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(getCmd)
}

var getCmd = &cobra.Command{
	Use:   "get SOURCE [LOCAL_DEST]",
	Short: "get files from HDFS to the local filesystem",
	RunE:  getRun,
	DisableFlagsInUseLine: true,
}

func getRun(cmd *cobra.Command, args []string) error {
	if len(args) == 0 || len(args) > 2 {
		return cmd.Help()
	}

	sources, nn, err := normalizePaths(args[0:1])
	if err != nil {
		return err
	}

	source := sources[0]
	var dest string
	if len(args) == 2 {
		dest = args[1]
	} else {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}

		_, name := path.Split(source)
		dest = filepath.Join(cwd, name)
	}

	client, err := getClient(nn)
	if err != nil {
		return err
	}

	err = client.Walk(source, func(p string, fi os.FileInfo, err error) error {
		fullDest := filepath.Join(dest, strings.TrimPrefix(p, source))

		if fi.IsDir() {
			err = os.Mkdir(fullDest, 0755)
			if err != nil {
				return err
			}
		} else {
			err = client.CopyToLocal(p, fullDest)
			return err
		}
		return nil
	})

	return err
}
