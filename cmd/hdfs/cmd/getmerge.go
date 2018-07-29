package cmd

import (
	"bytes"
	"io"
	"os"
	"path"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(getmergeCmd)
	getmergeCmd.PersistentFlags().BoolVarP(&getmergeNewlineOpt, "newline", "n", false, "add a newline after each file")
}

var (
	getmergeNewlineOpt bool
)

var getmergeCmd = &cobra.Command{
	Use:   "getmerge SOURCE LOCAL_DEST",
	Short: "get a directory from HDFS and merge the files into a single local file",
	RunE:  getmergeRun,
	DisableFlagsInUseLine: true,
}

func getmergeRun(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return cmd.Help()
	}

	dest := args[1]
	sources, nn, err := normalizePaths(args[0:1])
	if err != nil {
		return err
	}

	client, err := getClient(nn)
	if err != nil {
		return err
	}

	local, err := os.Create(dest)
	if err != nil {
		return err
	}

	source := sources[0]
	children, err := client.ReadDir(source)
	if err != nil {
		return err
	}

	readers := make([]io.Reader, 0, len(children))
	for _, child := range children {
		if child.IsDir() {
			continue
		}

		childPath := path.Join(source, child.Name())
		file, err := client.Open(childPath)
		if err != nil {
			return err
		}

		readers = append(readers, file)
		if getmergeNewlineOpt {
			readers = append(readers, bytes.NewBufferString("\n"))
		}
	}

	_, err = io.Copy(local, io.MultiReader(readers...))

	return err
}
