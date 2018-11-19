package hdfs

import (
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveFile(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/todelete")
	mkdirp(t, "/_test/todelete")
	touch(t, "/_test/todelete/deleteme")

	err := client.Remove("/_test/todelete/deleteme")
	require.NoError(t, err)

	fi, err := client.Stat("/_test/todelete/deleteme")
	assert.Nil(t, fi)
	assertPathError(t, err, "stat", "/_test/todelete/deleteme", os.ErrNotExist)
}

func TestRemoveEmptyDir(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/todelete")
	mkdirp(t, "/_test/todelete")

	err := client.Remove("/_test/todelete")
	require.NoError(t, err)

	fi, err := client.Stat("/_test/todelete")
	assert.Nil(t, fi)
	assertPathError(t, err, "stat", "/_test/todelete", os.ErrNotExist)
}

func TestRemoveNonEmptyDir(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/todelete")
	mkdirp(t, "/_test/todelete")
	touch(t, "/_test/todelete/dummy")

	err := client.Remove("/_test/todelete")
	assertPathError(t, err, "remove", "/_test/todelete", syscall.ENOTEMPTY)
	fi, err := client.Stat("/_test/todelete/dummy")
	require.NoError(t, err)
	assert.NotNil(t, fi)
}

func TestRemoveNonexistent(t *testing.T) {
	client := getClient(t)
	baleet(t, "/_test/nonexistent")

	err := client.Remove("/_test/nonexistent")
	assertPathError(t, err, "remove", "/_test/nonexistent", os.ErrNotExist)
}

func TestRemoveWithoutPermission(t *testing.T) {
	client := getClientForUser(t, "gohdfs2")

	mkdirp(t, "/_test/accessdenied")
	touch(t, "/_test/accessdenied/foo")

	err := client.Remove("/_test/accessdenied/foo")
	assertPathError(t, err, "remove", "/_test/accessdenied/foo", os.ErrPermission)
}

func TestRemoveAllEmptyDir(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/todelete")
	mkdirp(t, "/_test/todelete")

	err := client.RemoveAll("/_test/todelete")
	require.NoError(t, err)

	fi, err := client.Stat("/_test/todelete")
	assert.Nil(t, fi)
	assertPathError(t, err, "stat", "/_test/todelete", os.ErrNotExist)
}

func TestRemoveAllNonEmptyDir(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/todelete")
	mkdirp(t, "/_test/todelete")
	touch(t, "/_test/todelete/dummy")

	err := client.RemoveAll("/_test/todelete")
	require.NoError(t, err)

	fi, err := client.Stat("/_test/todelete")
	assert.Nil(t, fi)
	assertPathError(t, err, "stat", "/_test/todelete", os.ErrNotExist)
}

func TestRemoveAllNonexistent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")
	err := client.RemoveAll("/_test/nonexistent")
	require.NoError(t, err)
}

func TestRemoveAllWithoutPermission(t *testing.T) {
	client := getClientForUser(t, "gohdfs2")

	mkdirp(t, "/_test/accessdenied")
	touch(t, "/_test/accessdenied/foo")

	err := client.RemoveAll("/_test/accessdenied/foo")
	assertPathError(t, err, "remove", "/_test/accessdenied/foo", os.ErrPermission)
}
