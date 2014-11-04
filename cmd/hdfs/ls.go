package main

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"io"
	"os"
	"path"
	"strings"
	"text/tabwriter"
	"time"
)

func ls(paths []string, long, all bool) int {
	paths, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		fatal(err)
	}

	if len(paths) == 0 {
		paths = []string{userDir()}
	}

	files := make([]string, 0, len(paths))
	fileInfos := make([]os.FileInfo, 0, len(paths))
	dirs := make([]string, 0, len(paths))
	for _, p := range paths {
		fi, err := client.Stat(p)
		if err != nil {
			fatal(err)
		}

		if fi.IsDir() {
			dirs = append(dirs, p)
		} else {
			files = append(files, p)
			fileInfos = append(fileInfos, fi)
		}
	}

	if len(files) == 0 && len(dirs) == 1 {
		printDir(client, dirs[0], long, all)
	} else {
		if long {
			tw := defaultTabWriter()
			for i, p := range files {
				printLong(tw, p, fileInfos[i])
			}

			tw.Flush()
		} else {
			for _, p := range files {
				fmt.Println(p)
			}
		}

		for i, dir := range dirs {
			if i > 0 {
				fmt.Println()
			}

			fmt.Printf("%s/:\n", dir)
			printDir(client, dir, long, all)
		}
	}

	return 0
}

func printDir(client *hdfs.Client, dir string, long, all bool) {
	dirReader, err := client.Open(dir)
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
			dirInfo, err := client.Stat(dir)
			if err != nil {
				fatal(err)
			}

			parentPath := path.Join(dir, "..")
			parentInfo, err := client.Stat(parentPath)
			if err != nil {
				fatal(err)
			}

			printLong(tw, ".", dirInfo)
			printLong(tw, "..", parentInfo)
		} else {
			fmt.Println(".")
			fmt.Println("..")
		}
	}

	var partial []os.FileInfo
	for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
		if err != nil {
			fatal(err)
		}

		printFiles(tw, partial, long, all)

		if long {
			tw.Flush()
		}
	}
}

func printFiles(tw *tabwriter.Writer, files []os.FileInfo, long, all bool) {
	for _, file := range files {
		if !all && strings.HasPrefix(file.Name(), ".") {
			continue
		}

		if long {
			printLong(tw, file.Name(), file)
		} else {
			fmt.Println(file.Name())
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
