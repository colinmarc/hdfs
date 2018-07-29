package cmd

import (
	"errors"
	"os"
	"path"

	"github.com/colinmarc/hdfs"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(mvCmd)
	mvCmd.PersistentFlags().BoolVarP(&mvForceOpt, "force", "f", false, "do not prompt before overwriting")
	mvCmd.PersistentFlags().BoolVarP(&mvForceOpt, "no-target-directory", "T", false, "treat DEST as a normal file")
}

var mvCmd = &cobra.Command{
	Use:   "mv [-fT] SOURCE... DEST",
	Short: "move HDFS files",
	RunE:  mvRun,
	DisableFlagsInUseLine: true,
}

var (
	mvForceOpt   bool
	mvDestAsFile bool
)

func mvRun(cmd *cobra.Command, args []string) error {
	paths, nn, err := normalizePaths(args)
	if err != nil {
		return err
	}

	if len(paths) < 2 {
		return errors.New("both a source and destination are required")
	} else if hasGlob(paths[len(paths)-1]) {
		return errors.New("the destination must be a single path")
	}

	client, err := getClient(nn)
	if err != nil {
		return err
	}

	dest := paths[len(paths)-1]
	sources, err := expandPaths(client, paths[:len(paths)-1])
	if err != nil {
		return err
	}

	destInfo, err := client.Stat(dest)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	exists := !os.IsNotExist(err)
	if exists && !mvDestAsFile && destInfo.IsDir() {
		moveInto(client, sources, dest, mvForceOpt)
	} else {
		if len(sources) > 1 {
			return errors.New("can't move multiple sources into the same place")
		}

		moveTo(client, sources[0], dest, mvForceOpt)
	}
	return nil
}

func moveInto(client *hdfs.Client, sources []string, dest string, force bool) error {
	for _, source := range sources {
		_, name := path.Split(source)

		fullDest := path.Join(dest, name)
		err := moveTo(client, source, fullDest, force)
		if err != nil {
			return err
		}
	}
	return nil
}

func moveTo(client *hdfs.Client, source, dest string, force bool) error {
	sourceInfo, err := client.Stat(source)
	if err != nil {
		if pathErr, ok := err.(*os.PathError); ok {
			pathErr.Op = "rename"
		}
		return err
	}

	destInfo, err := client.Stat(dest)
	if err == nil {
		if destInfo.IsDir() && !sourceInfo.IsDir() {
			return errors.New("can't replace directory with non-directory")
		} else if !force {
			return &os.PathError{
				Op:   "rename",
				Path: dest,
				Err:  os.ErrExist,
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	err = client.Rename(source, dest)
	if err != nil {
		return err
	}
	return nil
}
