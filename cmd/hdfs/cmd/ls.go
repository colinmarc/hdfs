package cmd

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(lsCmd)
	lsCmd.PersistentFlags().BoolVarP(&lsListOpt, "list", "l", false, "use a long listing format")
	lsCmd.PersistentFlags().BoolVarP(&lsAllOpt, "all", "a", false, "show all entries, including hidden ones")
	lsCmd.PersistentFlags().BoolVarP(&lsHumanOpt, "human-readable", "h", false, "print human-readable sizes")
	lsCmd.Flags().BoolVarP(&lsHelpOpt, "help", "", false, "display help")
}

var (
	lsListOpt  bool
	lsAllOpt   bool
	lsHumanOpt bool
	lsHelpOpt  bool
)

var lsCmd = &cobra.Command{
	Use:   "ls [-lah] [FILE]...",
	Short: "list HDFS directory contents",
	RunE:  lsRun,
	DisableFlagsInUseLine: true,
}

func lsRun(cmd *cobra.Command, args []string) error {
	paths, client, err := getClientAndExpandedPaths(args)
	if err != nil {
		return err
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
			return err
		}

		if fi.IsDir() {
			dirs = append(dirs, p)
		} else {
			files = append(files, p)
			fileInfos = append(fileInfos, fi)
		}
	}

	if len(files) == 0 && len(dirs) == 1 {
		printDir(client, dirs[0], lsListOpt, lsAllOpt, lsHumanOpt)
	} else {
		if lsListOpt {
			tw := lsTabWriter()
			for i, p := range files {
				printLong(tw, p, fileInfos[i], lsHumanOpt)
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
			err := printDir(client, dir, lsListOpt, lsAllOpt, lsHumanOpt)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func printDir(client *hdfs.Client, dir string, long, all, humanReadable bool) error {
	dirReader, err := client.Open(dir)
	if err != nil {
		return err
	}

	var tw *tabwriter.Writer
	if long {
		tw = lsTabWriter()
		defer tw.Flush()
	}

	if all {
		if long {
			dirInfo, err := client.Stat(dir)
			if err != nil {
				return err
			}

			parentPath := path.Join(dir, "..")
			parentInfo, err := client.Stat(parentPath)
			if err != nil {
				return err
			}

			printLong(tw, ".", dirInfo, humanReadable)
			printLong(tw, "..", parentInfo, humanReadable)
		} else {
			fmt.Println(".")
			fmt.Println("..")
		}
	}

	var partial []os.FileInfo
	for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
		if err != nil {
			return err
		}

		printFiles(tw, partial, long, all, humanReadable)
	}

	if long {
		tw.Flush()
	}
	return nil
}

func printFiles(tw *tabwriter.Writer, files []os.FileInfo, long, all, humanReadable bool) {
	for _, file := range files {
		if !all && strings.HasPrefix(file.Name(), ".") {
			continue
		}

		if long {
			printLong(tw, file.Name(), file, humanReadable)
		} else {
			fmt.Println(file.Name())
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
