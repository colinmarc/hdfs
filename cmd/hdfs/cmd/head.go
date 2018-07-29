package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/colinmarc/hdfs"
	"github.com/spf13/cobra"
)

const tailSearchSize int64 = 16384

func init() {
	rootCmd.AddCommand(headCmd)
	headCmd.PersistentFlags().Int64VarP(&headLinesOpt, "lines", "n", -1, "print the first N lines of the files")
	headCmd.PersistentFlags().Int64VarP(&headBytesOpt, "bytes", "c", -1, "print the first N lines of the files")

	rootCmd.AddCommand(tailCmd)
	tailCmd.PersistentFlags().Int64VarP(&headLinesOpt, "lines", "n", -1, "print the first N lines of the files")
	tailCmd.PersistentFlags().Int64VarP(&headBytesOpt, "bytes", "c", -1, "print the first N lines of the files")
}

var (
	headLinesOpt int64
	headBytesOpt int64
)

var headCmd = &cobra.Command{
	Use:   "head [-n LINES | -c BYTES] SOURCE...",
	Short: "output the first part of HDFS files",
	RunE:  headRun,
	DisableFlagsInUseLine: true,
}

var tailCmd = &cobra.Command{
	Use:   "tail [-n LINES | -c BYTES] SOURCE...",
	Short: "output the last part of HDFS files",
	RunE:  tailRun,
	DisableFlagsInUseLine: true,
}

func headRun(cmd *cobra.Command, args []string) error {
	return printSection(args, false)
}

func tailRun(cmd *cobra.Command, args []string) error {
	return printSection(args, true)
}

func printSection(args []string, fromEnd bool) error {
	if headLinesOpt != -1 && headBytesOpt != -1 {
		return errors.New("you can't specify both -n and -c")
	} else if headLinesOpt == -1 && headBytesOpt == -1 {
		headLinesOpt = 10
	}

	expanded, client, err := getClientAndExpandedPaths(args)
	if err != nil {
		return err
	}

	for _, p := range expanded {
		file, err := client.Open(p)
		if err != nil || file.Stat().IsDir() {
			if err == nil && file.Stat().IsDir() {
				err = &os.PathError{
					Op:   "open",
					Path: p,
					Err:  errors.New("file is a directory"),
				}
			}
			return err
		}

		if len(expanded) > 1 {
			fmt.Fprintf(os.Stderr, "%s:\n", file.Name())
		}

		if headLinesOpt != -1 {
			if fromEnd {
				err = tailLines(file, headLinesOpt)
			} else {
				err = headLines(file, headLinesOpt)
			}
		} else {
			var offset int64
			if fromEnd {
				offset = file.Stat().Size() - headBytesOpt
			}

			reader := io.NewSectionReader(file, offset, headBytesOpt)
			_, err = io.Copy(os.Stdout, reader)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func headLines(file *hdfs.FileReader, numLines int64) error {
	reader := bufio.NewReader(file)

	var newlines, offset int64
	for newlines < numLines {
		b, err := reader.ReadByte()
		if err == io.EOF {
			offset = -1
			break
		} else if err != nil {
			return err
		}

		if b == '\n' {
			newlines++
		}

		offset++
	}

	_, err := file.Seek(0, 0)
	if err != nil {
		return err
	}

	if offset < 0 {
		_, err = io.Copy(os.Stdout, file)
	} else {
		_, err = io.CopyN(os.Stdout, file, offset)
	}
	return err
}

func tailLines(file *hdfs.FileReader, numLines int64) error {
	fileSize := file.Stat().Size()
	searchPoint := file.Stat().Size() - tailSearchSize
	if searchPoint < 0 {
		searchPoint = 0
	}
	readSize := tailSearchSize
	if readSize > fileSize {
		readSize = fileSize
	}

	var printOffset int64
	for searchPoint >= 0 {
		section := bufio.NewReader(io.NewSectionReader(file, searchPoint, readSize))
		off := searchPoint
		newlines := make([]int64, 0, tailSearchSize/64)

		b, err := section.ReadByte()
		for err == nil {
			if b == '\n' && (off+1 != fileSize) {
				newlines = append(newlines, off)
			}

			off++
			b, err = section.ReadByte()
		}

		if err != nil && err != io.EOF {
			return err
		}

		foundNewlines := int64(len(newlines))
		if foundNewlines >= numLines {
			printOffset = newlines[foundNewlines-numLines] + 1
			break
		}

		numLines -= foundNewlines
		searchPoint -= tailSearchSize
	}

	_, err := file.Seek(printOffset, 0)
	if err != nil {
		return err
	}

	_, err = io.Copy(os.Stdout, file)
	return err
}
