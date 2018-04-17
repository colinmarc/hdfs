package hdfs

import (
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Walk does the exact same thing as filepath.Walk (and is mostly copied from there)
func (client *Client) Walk(root string, visit filepath.WalkFunc) error {
	rootInfo, err := client.Stat(root)
	if err != nil {
		err = visit(root, nil, err)
	} else {
		err = client.walk(root, rootInfo, visit)
	}
	if err == filepath.SkipDir {
		return nil
	}
	return err
}

func (client *Client) walk(path string, info os.FileInfo, visit filepath.WalkFunc) error {
	err := visit(path, info, nil)
	if err != nil {
		if info.IsDir() && err == filepath.SkipDir {
			return nil
		}
		return err
	}

	if !info.IsDir() {
		return nil
	}

	names, err := client.readDirNames(path)
	if err != nil {
		return visit(path, info, err)
	}

	for _, name := range names {
		filename := filepath.Join(path, name)
		fileInfo, err := client.Stat(filename)
		if err != nil {
			if err := visit(filename, fileInfo, err); err != nil && err != filepath.SkipDir {
				return err
			}
		} else {
			err = client.walk(filename, fileInfo, visit)
			if err != nil {
				if !fileInfo.IsDir() || err != filepath.SkipDir {
					return err
				}
			}
		}
	}
	return nil
}

func (client *Client) readDirNames(dir string) ([]string, error) {
	dirReader, err := client.Open(dir)
	if err != nil {
		return nil, err
	}

	// read the dir entries by chunks of a few hundreds
	names, err := readDirNamesByChunks(dirReader, 500)

	if err != nil {
		return nil, err
	}

	sort.Strings(names)
	return names, nil
}

func readDirNamesByChunks(dirReader *FileReader, chunkSize int) ([]string, error) {

	var toRet []string
	var partial []string
	var err error

	if chunkSize <= 0 {
		return dirReader.Readdirnames(chunkSize)
	}

	for ; err != io.EOF; partial, err = dirReader.Readdirnames(chunkSize) {
		if err != nil {
			return nil, err
		}
		toRet = append(toRet, partial...)
	}
	return toRet, nil
}
