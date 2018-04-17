package hdfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadDirNames(t *testing.T) {
	c := getClient(t)
	entries, err := c.readDirNames("/_test/")
	assert.Nil(t, err, "unexpected error")
	assert.False(t, len(entries) == 0, "did not list anything")
	assert.Contains(t, entries, "mobydick.txt", "did not find expected file")
	assert.Contains(t, entries, "foo.txt", "did not find expected file")
}

func TestReadDirNamesByChunks(t *testing.T) {
	c := getClient(t)
	dirReader, err := c.Open("/_test/")

	assert.Nil(t, err, "unexpected error")

	singleRun, err := readDirNamesByChunks(dirReader, -1)
	assert.Nil(t, err, "unexpected error")

	dirReader, err = c.Open("/_test/")
	assert.Nil(t, err, "unexpected error")

	byChunks, err := readDirNamesByChunks(dirReader, 1)
	assert.Nil(t, err, "unexpected error")

	assert.Equal(t, singleRun, byChunks, "discrepancy in listed content")
}

func TestWalk(t *testing.T) {
	c := getClient(t)

	c.Mkdir("/_test/walk", os.ModePerm)
	c.Mkdir("/_test/walk/dir", os.ModePerm)
	c.Mkdir("/_test/walk/dir/subdir", os.ModePerm)
	c.Create("/_test/walk/walkfile")
	c.Create("/_test/walk/dir/walkfile1")
	c.Create("/_test/walk/dir/walkfile2")
	c.Create("/_test/walk/dir/subdir/walkfile1")
	c.Create("/_test/walk/dir/subdir/walkfile2")

	paths := make([]string, 0, 8)

	err := c.Walk("/_test/walk/", walkFnTest(&paths))
	assert.Nil(t, err, "unexpected error")

	expected := []string{
		"/_test/walk/",
		"/_test/walk/dir",
		"/_test/walk/dir/subdir",
		"/_test/walk/dir/subdir/walkfile1",
		"/_test/walk/dir/subdir/walkfile2",
		"/_test/walk/dir/walkfile1",
		"/_test/walk/dir/walkfile2",
		"/_test/walk/walkfile"}

	assert.Equal(t, expected, paths, "discrepancy between expected and walked paths.")

}

func TestWalkError(t *testing.T) {
	c := getClient(t)
	errors := make([]error, 0, 1)
	c.Walk("/not_existing", walkErrorFn(&errors))
	assert.Equal(t, 1, len(errors), "expected a single error")
}

func walkFnTest(encounteredPaths *[]string) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		*encounteredPaths = append(*encounteredPaths, path)
		return nil
	}
}

func walkErrorFn(errors *[]error) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			*errors = append(*errors, err)
		}
		return nil
	}
}
