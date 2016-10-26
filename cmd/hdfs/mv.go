package main

import (
	"os"
	"path"

	"github.com/colinmarc/hdfs"
)

func mv(paths []string, force, treatDestAsFile bool) {
	paths, nn, err := normalizePaths(paths)
	if err != nil {
		fatal(err)
	}

	if len(paths) < 2 {
		fatalWithUsage("Both a source and destination are required.")
	} else if hasGlob(paths[len(paths)-1]) {
		fatal("The destination must be a single path.")
	}

	client, err := getClient(nn)
	if err != nil {
		fatal(err)
	}

	dest := paths[len(paths)-1]
	sources, err := expandPaths(client, paths[:len(paths)-1])
	if err != nil {
		fatal(err)
	}

	destInfo, err := client.Stat(dest)
	if err != nil && !os.IsNotExist(err) {
		fatal(err)
	}

	exists := !os.IsNotExist(err)
	if exists && !treatDestAsFile && destInfo.IsDir() {
		moveInto(client, sources, dest, force)
	} else {
		if len(sources) > 1 {
			fatal("Can't move multiple sources into the same place.")
		}

		moveTo(client, sources[0], dest, force)
	}
}

func moveInto(client *hdfs.Client, sources []string, dest string, force bool) {
	for _, source := range sources {
		_, name := path.Split(source)

		fullDest := path.Join(dest, name)
		moveTo(client, source, fullDest, force)
	}
}

func moveTo(client *hdfs.Client, source, dest string, force bool) {
	sourceInfo, err := client.Stat(source)
	if err != nil {
		if pathErr, ok := err.(*os.PathError); ok {
			pathErr.Op = "rename"
		}

		fatal(err)
	}

	destInfo, err := client.Stat(dest)
	if err == nil {
		if destInfo.IsDir() && !sourceInfo.IsDir() {
			fatal("Can't replace directory with non-directory.")
		} else if !force {
			fatal(&os.PathError{"rename", dest, os.ErrExist})
		}
	} else if !os.IsNotExist(err) {
		fatal(err)
	}

	err = client.Rename(source, dest)
	if err != nil {
		fatal(err)
	}
}
