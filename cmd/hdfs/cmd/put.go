package cmd

import (
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/colinmarc/hdfs"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(putCmd)
}

var putCmd = &cobra.Command{
	Use:   "put LOCAL_SOURCE DEST",
	Short: "put files from local filesystem to HDFS",
	RunE:  putRun,
	DisableFlagsInUseLine: true,
}

func putRun(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return cmd.Usage()
	}

	dests, nn, err := normalizePaths(args[1:])
	if err != nil {
		return err
	}

	dest := dests[0]
	source, err := filepath.Abs(args[0])
	if err != nil {
		return err
	}

	client, err := getClient(nn)
	if err != nil {
		return err
	}

	if filepath.Base(source) == "-" {
		return putFromStdin(client, dest)
	}
	return putFromFile(client, source, dest)
}

func putFromStdin(client *hdfs.Client, dest string) error {
	// If the destination exists, regardless of what it is, bail out.
	_, err := client.Stat(dest)
	if err == nil {
		return &os.PathError{
			Op:   "put",
			Path: dest,
			Err:  os.ErrExist}
	} else if !os.IsNotExist(err) {
		return err
	}

	mode := 0755 | os.ModeDir
	parentDir := filepath.Dir(dest)
	if parentDir != "." && parentDir != "/" {
		if err := client.MkdirAll(parentDir, mode); err != nil {
			return err
		}
	}

	writer, err := client.Create(dest)
	if err != nil {
		return err
	}
	defer writer.Close()

	_, err = io.Copy(writer, os.Stdin)
	return err
}

func putFromFile(client *hdfs.Client, source string, dest string) error {
	// If the destination is an existing directory, place it inside. Otherwise,
	// the destination is really the parent directory, and we need to rename the
	// source directory as we copy.
	existing, err := client.Stat(dest)
	if err == nil {
		if existing.IsDir() {
			dest = path.Join(dest, filepath.Base(source))
		} else {
			return &os.PathError{
				Op:   "mkdir",
				Path: dest,
				Err:  os.ErrExist,
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	mode := 0755 | os.ModeDir
	err = filepath.Walk(source, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(source, p)
		if err != nil {
			return err
		}

		fullDest := path.Join(dest, rel)
		if fi.IsDir() {
			client.Mkdir(fullDest, mode)
		} else {
			writer, err := client.Create(fullDest)
			if err != nil {
				return err
			}

			defer writer.Close()
			reader, err := os.Open(p)
			if err != nil {
				return err
			}

			defer reader.Close()
			_, err = io.Copy(writer, reader)
			if err != nil {
				return err
			}
		}

		return nil
	})

	return err
}
