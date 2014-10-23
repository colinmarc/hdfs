package main

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"os"
)

func ls(paths []string, all, long bool) {
	paths, nn, err := normalizePaths(paths)
	if err != nil {
		fatal(err)
	}

	client, err := getClient(nn)
	if err != nil {
		fatal(err)
	}

	expanded, err := expandPaths(client, paths)
	if err != nil {
		fatal(err)
	}

	files := make([]os.FileInfo, 0, len(expanded))
	dirs := make([]string, 0, len(expanded))
	for _, p := range expanded {
		fi, err := stat(client, p)
		if err != nil {
			fatal(err)
		}

		if fi.IsDir() {
			dirs = append(dirs, p)
		} else {
			files = append(files, fi)
		}
	}

	if len(files) == 0 && len(dirs) == 1 {
		printDir(client, dirs[0], all, long)
	} else {
		printFiles(files, long)

		for _, dir := range dirs {
			fmt.Printf("\n%s/:\n", dir)
			printDir(client, dir, all, long)
		}
	}
}

func printDir(client *hdfs.Client, dir string, all, long bool) {
	files, err := readDir(client, dir, "")
	if err != nil {
		fatal(err)
	}

	printFiles(files, long)
}

func printFiles(files []os.FileInfo, long bool) {
	for _, file := range files {
		fmt.Println(file.Name())
	}
}
