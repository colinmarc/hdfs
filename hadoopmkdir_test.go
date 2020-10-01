package hdfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var mode = 0777 | os.ModeDir

func TestMkdir(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/dir2")

	err := client.Mkdir("/_test/dir2", mode)
	require.NoError(t, err)

	fi, err := client.Stat("/_test/dir2")
	require.NoError(t, err)
	assert.True(t, fi.IsDir())
}

func TestMkdirExists(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/existingdir")

	err := client.Mkdir("/_test/existingdir", mode)
	assertPathError(t, err, "mkdir", "/_test/existingdir", os.ErrExist)
}

func TestMkdirWithoutParent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")

	err := client.Mkdir("/_test/nonexistent/foo", mode)
	assertPathError(t, err, "mkdir", "/_test/nonexistent/foo", os.ErrNotExist)

	_, err = client.Stat("/_test/nonexistent/foo")
	assertPathError(t, err, "stat", "/_test/nonexistent/foo", os.ErrNotExist)

	_, err = client.Stat("/_test/nonexistent")
	assertPathError(t, err, "stat", "/_test/nonexistent", os.ErrNotExist)
}

func TestMkdirAll(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/dir3")

	err := client.MkdirAll("/_test/dir3/foo", mode)
	require.NoError(t, err)

	fi, err := client.Stat("/_test/dir3/foo")
	require.NoError(t, err)
	assert.True(t, fi.IsDir())

	fi, err = client.Stat("/_test/dir3")
	require.NoError(t, err)
	assert.True(t, fi.IsDir())
	assert.EqualValues(t, 0, fi.Size())
}

func TestMkdirAllExists(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/dir4")

	err := client.MkdirAll("/_test/dir4/foo", mode)
	require.NoError(t, err)

	err = client.MkdirAll("/_test/dir4/foo", mode)
	require.NoError(t, err)
}

func TestMkdirWIthoutPermission(t *testing.T) {
	client := getClient(t)
	client2 := getClientForUser(t, "gohdfs2")

	mkdirp(t, "/_test/accessdenied")

	err := client2.Mkdir("/_test/accessdenied/dir", mode)
	assertPathError(t, err, "mkdir", "/_test/accessdenied/dir", os.ErrPermission)

	_, err = client.Stat("/_test/accessdenied/dir")
	assertPathError(t, err, "stat", "/_test/accessdenied/dir", os.ErrNotExist)
}

func TestMkdirAllWIthoutPermission(t *testing.T) {
	client := getClient(t)
	client2 := getClientForUser(t, "gohdfs2")

	mkdirp(t, "/_test/accessdenied")

	err := client2.Mkdir("/_test/accessdenied/dir2/foo", mode)
	assertPathError(t, err, "mkdir", "/_test/accessdenied/dir2/foo", os.ErrPermission)

	_, err = client.Stat("/_test/accessdenied/dir2/foo")
	assertPathError(t, err, "stat", "/_test/accessdenied/dir2/foo", os.ErrNotExist)

	_, err = client.Stat("/_test/accessdenied/dir2")
	assertPathError(t, err, "stat", "/_test/accessdenied/dir2", os.ErrNotExist)
}
