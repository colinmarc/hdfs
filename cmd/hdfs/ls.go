package main

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"os"
	"path"
	"strings"
	"text/tabwriter"
	"time"
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
		var tw *tabwriter.Writer
		if long {
			tw = defaultTabWriter()
			defer tw.Flush()
		}

		printFiles(tw, files, long, all)

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

	var tw *tabwriter.Writer
	if long {
		tw = defaultTabWriter()
		defer tw.Flush()
	}

	if all {
		if long {
			dot, err := stat(client, dir)
			if err != nil {
				fatal(err)
			}

			dotdot, err := stat(client, path.Join(dir, ".."))
			if err != nil {
				fatal(err)
			}

			printLong(tw, ".", dot)
			printLong(tw, "..", dotdot)
		} else {
			fmt.Println(".")
			fmt.Println("..")
		}
	}

	printFiles(tw, files, long, all)
}

func printFiles(tw *tabwriter.Writer, files []os.FileInfo, long, all bool) {
	for _, file := range files {
		if all || !strings.HasPrefix(file.Name(), ".") {
			if long {
				printLong(tw, file.Name(), file)
			} else {
				fmt.Println(file.Name())
			}
		}
	}
}

func printLong(tw *tabwriter.Writer, name string, info os.FileInfo) {
	fi := info.(*hdfs.FileInfo)
	// mode owner group size date(\w tab) time/year name
	mode := fi.Mode().String()
	owner := fi.Owner()
	group := fi.OwnerGroup()
	size := fi.Size()

	modtime := fi.ModTime()
	date := modtime.Format("Jan\t2")
	var timeOrYear string
	if modtime.Year() == time.Now().Year() {
		timeOrYear = modtime.Format("15:04")
	} else {
		timeOrYear = string(modtime.Year())
	}

	fmt.Fprintf(tw, "%s \t%s \t %s \t %d \t%s \t%s \t%s\n",
		mode, owner, group, size, date, timeOrYear, name)
}

func defaultTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 3, 0, 0, ' ', tabwriter.AlignRight|tabwriter.TabIndent)
}
