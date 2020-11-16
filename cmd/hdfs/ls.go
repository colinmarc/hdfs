package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/colinmarc/hdfs/v2"
)

var recursionLevel = 0

func ls(paths []string, long, all, humanReadable bool, recursive bool, selfOnly bool) {
	paths, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		fatal(err)
	}

	if len(paths) == 0 {
		paths = []string{userDir(client)}
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
		printDir(client, dirs[0], long, all, humanReadable, recursive, selfOnly)
	} else {
		if long {
			tw := lsTabWriter()
			for i, p := range files {
				printLong(tw, p, fileInfos[i], humanReadable)
			}

			tw.Flush()
		} else {
			for _, p := range files {
				fmt.Println(p)
			}
		}

		if selfOnly {
			for _, dir := range dirs {
				tw := lsTabWriter()
				dirstat, err := client.Stat(dir)
				dirstats := []os.FileInfo{dirstat}
				if err != nil {
					fatal(err)
				}
				printFiles(client, tw, dir, dirstats, long, all, humanReadable, recursive, selfOnly)
				tw.Flush()
			}
		} else {
			for i, dir := range dirs {
				if i > 0 || len(files) > 0 {
					fmt.Println()
				}

				fmt.Printf("%s/:\n", dir)
				printDir(client, dir, long, all, humanReadable, recursive, selfOnly)
			}
		}
	}
}

func printDir(client *hdfs.Client, dir string, long, all, humanReadable bool, recursive bool, selfOnly bool) {
	dirReader, err := client.Open(dir)
	if err != nil {
		fatal(err)
	}

	var tw *tabwriter.Writer
	if long {
		tw = lsTabWriter()
		defer tw.Flush()
	}

	if selfOnly {
		if long {
			dirInfo, err := client.Stat(dir)
			if err != nil {
				fatal(err)
			}
			printLong(tw, dir, dirInfo, humanReadable)
			tw.Flush()
		} else {
			fmt.Println(dir)
		}
		return
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

			printLong(tw, ".", dirInfo, humanReadable)
			printLong(tw, "..", parentInfo, humanReadable)
		} else {
			fmt.Println(".")
			fmt.Println("..")
		}
	}

	var partial, files []os.FileInfo
	var fileCount = 0
	for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
		if err != nil {
			fatal(err)
		}
		fileCount += len(partial)
		files = append(files, partial...)
	}

	if recursive {
		if long {
			if recursionLevel > 0 {
				fmt.Fprintln(tw)
			}
			fmt.Fprintf(tw, "%s:\n", dir)
			fmt.Fprintln(tw, "total", fileCount)
		} else {
			if recursionLevel > 0 {
				fmt.Println()
			}
			fmt.Printf("%s:\n", dir)
			fmt.Println("total", fileCount)
		}
		recursionLevel++
	}

	printFiles(client, tw, dir, files, long, all, recursive, humanReadable, selfOnly)

	if long {
		tw.Flush()
	}
}

func printFiles(client *hdfs.Client, tw *tabwriter.Writer, dir string, files []os.FileInfo, long, all, recursive, humanReadable, selfOnly bool) {
	for _, file := range files {
		if !all && strings.HasPrefix(file.Name(), ".") {
			continue
		}

		filename := file.Name()
		if selfOnly {
			splittedDir := strings.Split(dir, "/")
			filename = strings.Join(splittedDir[:len(splittedDir)-1], "/") + "/" + filename
		}

		if long {
			printLong(tw, filename, file, humanReadable)
			tw.Flush()
		} else {
			fmt.Println(filename)
		}
	}

	for _, file := range files {
		if recursive && file.IsDir() {
			if dir == "/" {
				printDir(client, dir+file.Name(), long, all, humanReadable, recursive, selfOnly)
			} else {
				printDir(client, dir+"/"+file.Name(), long, all, humanReadable, recursive, selfOnly)
			}
		}
	}
}

func printLong(tw *tabwriter.Writer, name string, info os.FileInfo, humanReadable bool) {
	fi := info.(*hdfs.FileInfo)
	// mode owner group size date(\w tab) time/year name
	mode := fi.Mode().String()
	owner := fi.Owner()
	group := fi.OwnerGroup()
	size := strconv.FormatInt(fi.Size(), 10)
	if humanReadable {
		size = formatBytes(uint64(fi.Size()))
	}

	modtime := fi.ModTime()
	date := modtime.Format("Jan _2")
	var timeOrYear string
	if modtime.Year() == time.Now().Year() {
		timeOrYear = modtime.Format("15:04")
	} else {
		timeOrYear = modtime.Format("2006")
	}

	fmt.Fprintf(tw, "%s \t%s \t %s \t %s \t%s \t%s \t%s\n",
		mode, owner, group, size, date, timeOrYear, name)
}

func lsTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 3, 8, 0, ' ', tabwriter.AlignRight|tabwriter.TabIndent)
}
