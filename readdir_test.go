package hdfs

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadDir(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/fulldir")
	mkdirp(t, "/_test/fulldir/dir")
	touch(t, "/_test/fulldir/1")
	touch(t, "/_test/fulldir/2")
	touch(t, "/_test/fulldir/3")

	res, err := client.ReadDir("/_test/fulldir")
	require.NoError(t, err)
	require.Equal(t, len(res), 4)

	assert.EqualValues(t, "1", res[0].Name())
	assert.False(t, res[0].IsDir())

	assert.EqualValues(t, "2", res[1].Name())
	assert.False(t, res[1].IsDir())

	assert.EqualValues(t, "3", res[2].Name())
	assert.False(t, res[2].IsDir())

	assert.EqualValues(t, "dir", res[3].Name())
	assert.True(t, res[3].IsDir())
}

func TestReadDirMany(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/hugedir")
	for i := 1; i <= 1000; i++ {
		touch(t, fmt.Sprintf("/_test/hugedir/%d", i))
	}

	res, err := client.ReadDir("/_test/hugedir")
	require.NoError(t, err)
	require.Equal(t, len(res), 1000)
}

func TestReadDirTrailingSlash(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/fulldir2")
	mkdirp(t, "/_test/fulldir2/dir")
	touch(t, "/_test/fulldir2/1")
	touch(t, "/_test/fulldir2/2")
	touch(t, "/_test/fulldir2/3")

	res, err := client.ReadDir("/_test/fulldir2/")
	require.NoError(t, err)
	require.Equal(t, len(res), 4)

	assert.EqualValues(t, "1", res[0].Name())
	assert.False(t, res[0].IsDir())

	assert.EqualValues(t, "2", res[1].Name())
	assert.False(t, res[1].IsDir())

	assert.EqualValues(t, "3", res[2].Name())
	assert.False(t, res[2].IsDir())

	assert.EqualValues(t, "dir", res[3].Name())
	assert.True(t, res[3].IsDir())
}

func TestReadEmptyDir(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/emptydir")
	mkdirp(t, "/_test/emptydir")

	res, err := client.ReadDir("/_test/emptydir")
	assert.NoError(t, err)
	assert.EqualValues(t, 0, len(res))
}

func TestReadDirNonexistent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")

	res, err := client.ReadDir("/_test/nonexistent")
	assertPathError(t, err, "readdir", "/_test/nonexistent", os.ErrNotExist)
	assert.Nil(t, res)
}

func TestReadDirWithoutPermission(t *testing.T) {
	mkdirp(t, "/_test/accessdenied")
	touch(t, "/_test/accessdenied/foo")

	client := getClientForUser(t, "other")

	res, err := client.ReadDir("/_test/accessdenied")
	assertPathError(t, err, "readdir", "/_test/accessdenied", os.ErrPermission)
	assert.Nil(t, res)
}
