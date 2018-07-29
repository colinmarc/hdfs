package cmd

import (
	"errors"
	"io"
	"os"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(catCmd)
}

var catCmd = &cobra.Command{
	Use:   "cat",
	Short: "concatenate HDFS files and print on the standard output",
	RunE:  catRun,
}

func catRun(cmd *cobra.Command, args []string) error {
	expanded, client, err := getClientAndExpandedPaths(args)
	if err != nil {
		return err
	}

	readers := make([]io.Reader, 0, len(expanded))
	for _, p := range expanded {
		file, err := client.Open(p)
		if err != nil {
			return err
		} else if file.Stat().IsDir() {
			return &os.PathError{
				Op:   "cat",
				Path: p,
				Err:  errors.New("file is a directory"),
			}
		}

		readers = append(readers, file)
	}

	_, err = io.Copy(os.Stdout, io.MultiReader(readers...))
	if err != nil {
		return err
	}

	return nil
}
