package main

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"os"
	"strings"
)

func ls(paths []string, long, all bool) {
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
		printDir(client, dirs[0], long, all)
	} else {
		printFiles(files, long, all)

		for _, dir := range dirs {
			fmt.Printf("\n%s/:\n", dir)
			printDir(client, dir, long, all)
		}
	}
}

func printDir(client *hdfs.Client, dir string, long, all bool) {
	files, err := readDir(client, dir, "")
	if err != nil {
		fatal(err)
	}

	if all {
		fmt.Println(".")
		fmt.Println("..")
	}

	printFiles(files, long, all)
}

func printFiles(files []os.FileInfo, long, all bool) {
	for _, file := range files {
		if all || !strings.HasPrefix(file.Name(), ".") {
			fmt.Println(file.Name())
		}
	}
}
