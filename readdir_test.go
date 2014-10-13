package hdfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestReadDir(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/fulldir")
	mkdirp(t, "/_test/fulldir/dir")
	touch(t, "/_test/fulldir/1")
	touch(t, "/_test/fulldir/2")
	touch(t, "/_test/fulldir/3")

	res, err := client.ReadDir("/_test/fulldir")
	assert.Nil(t, err)
	require.Equal(t, len(res), 4)

	assert.Equal(t, "/_test/fulldir/1", res[0].Name())
	assert.False(t, res[0].IsDir())

	assert.Equal(t, "/_test/fulldir/2", res[1].Name())
	assert.False(t, res[1].IsDir())

	assert.Equal(t, "/_test/fulldir/3", res[2].Name())
	assert.False(t, res[2].IsDir())

	assert.Equal(t, "/_test/fulldir/dir", res[3].Name())
	assert.True(t, res[3].IsDir())
}

func TestReadDirTrailingSlash(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/fulldir2")
	mkdirp(t, "/_test/fulldir2/dir")
	touch(t, "/_test/fulldir2/1")
	touch(t, "/_test/fulldir2/2")
	touch(t, "/_test/fulldir2/3")

	res, err := client.ReadDir("/_test/fulldir2/")
	assert.Nil(t, err)
	require.Equal(t, len(res), 4)

	assert.Equal(t, "/_test/fulldir2/1", res[0].Name())
	assert.False(t, res[0].IsDir())

	assert.Equal(t, "/_test/fulldir2/2", res[1].Name())
	assert.False(t, res[1].IsDir())

	assert.Equal(t, "/_test/fulldir2/3", res[2].Name())
	assert.False(t, res[2].IsDir())

	assert.Equal(t, "/_test/fulldir2/dir", res[3].Name())
	assert.True(t, res[3].IsDir())
}

func TestReadEmptyDir(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/emptydir")
	mkdirp(t, "/_test/emptydir")

	res, err := client.ReadDir("/_test/emptydir")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(res))
}

func TestReadDirNonexistent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")

	res, err := client.ReadDir("/_test/nonexistent")
	assert.Equal(t, os.ErrNotExist, err)
	assert.Nil(t, res)
}

