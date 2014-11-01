package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/colinmarc/hdfs"
	"io"
	"os"
)

var tailSearchSize int64 = 16384

func cat(paths []string) int {
	expanded, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		fatal(err)
	}

	readers := make([]io.Reader, 0, len(expanded))
	for _, p := range expanded {
		file, err := client.Open(p)
		if err != nil {
			fatal(err)
		} else if file.Stat().IsDir() {
			fatal(&os.PathError{"cat", p, errors.New("file is a directory")})
		}

		readers = append(readers, file)
	}

	_, err = io.Copy(os.Stdout, io.MultiReader(readers...))
	if err != nil {
		fatal(err)
	}

	return 0
}

func printSection(paths []string, numLines, numBytes int64, fromEnd bool) int {
	if numLines != -1 && numBytes != -1 {
		fatal("You can't specify both -n and -c.")
	} else if numLines == -1 && numBytes == -1 {
		numLines = 10
	}

	expanded, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		fatal(err)
	}

	status := 0
	for _, p := range expanded {
		file, err := client.Open(p)
		if err != nil || file.Stat().IsDir() {
			if err == nil && file.Stat().IsDir() {
				err = &os.PathError{"open", p, errors.New("file is a directory")}
			}

			fmt.Fprintln(os.Stderr, err)
			status = 1
			continue
		}

		if len(expanded) > 1 {
			fmt.Fprintf(os.Stderr, "%s:\n", file.Name())
		}

		if numLines != -1 {
			if fromEnd {
				tailLines(file, numLines)
			} else {
				headLines(file, numLines)
			}
		} else {
			var offset int64 = 0
			if fromEnd {
				offset = file.Stat().Size() - numBytes
			}

			reader := io.NewSectionReader(file, offset, numBytes)
			io.Copy(os.Stdout, reader)
		}
	}

	return status
}

func headLines(file *hdfs.FileReader, numLines int64) {
	reader := bufio.NewReader(file)

	var newlines, offset int64
	for newlines < numLines {
		b, err := reader.ReadByte()
		if err == io.EOF {
			offset = -1
			break
		} else if err != nil {
			fatal(err)
		}

		if b == '\n' {
			newlines++
		}

		offset++
	}

	_, err := file.Seek(0, 0)
	if err != nil {
		fatal(err)
	}

	if offset < 0 {
		io.Copy(os.Stdout, file)
	} else {
		io.CopyN(os.Stdout, file, offset)
	}
}

func tailLines(file *hdfs.FileReader, numLines int64) {
	fileSize := file.Stat().Size()
	searchPoint := file.Stat().Size() - tailSearchSize
	if searchPoint < 0 {
		searchPoint = 0
	}

	var printOffset int64 = 0
	for searchPoint >= 0 {
		section := bufio.NewReader(io.NewSectionReader(file, searchPoint, tailSearchSize))
		off := searchPoint
		newlines := make([]int64, 0, tailSearchSize/64)

		b, err := section.ReadByte()
		for err == nil {
			if b == '\n' && (off+1 != fileSize) {
				newlines = append(newlines, off)
			}

			off += 1
			b, err = section.ReadByte()
		}

		if err != nil && err != io.EOF {
			fatal(err)
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
		fatal(err)
	}

	io.Copy(os.Stderr, file)
}
