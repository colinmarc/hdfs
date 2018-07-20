package hdfs

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStat(t *testing.T) {
	client := getClient(t)

	resp, err := client.Stat("/_test/foo.txt")
	require.NoError(t, err)

	assert.EqualValues(t, "foo.txt", resp.Name())
	assert.False(t, resp.IsDir())
	assert.EqualValues(t, 4, resp.Size())
	assert.EqualValues(t, time.Now().Year(), resp.ModTime().Year())
	assert.EqualValues(t, time.Now().Month(), resp.ModTime().Month())
}

func TestStatEmptyFile(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/emptyfile2")

	resp, err := client.Stat("/_test/emptyfile2")
	require.NoError(t, err)

	assert.EqualValues(t, "emptyfile2", resp.Name())
	assert.False(t, resp.IsDir())
	assert.EqualValues(t, 0, resp.Size())
	assert.EqualValues(t, time.Now().Year(), resp.ModTime().Year())
	assert.EqualValues(t, time.Now().Month(), resp.ModTime().Month())
}

func TestStatNotExistent(t *testing.T) {
	client := getClient(t)

	resp, err := client.Stat("/_test/nonexistent")
	assertPathError(t, err, "stat", "/_test/nonexistent", os.ErrNotExist)
	assert.Nil(t, resp)
}

func TestStatDir(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/dir")

	resp, err := client.Stat("/_test/dir")
	require.NoError(t, err)

	assert.EqualValues(t, "dir", resp.Name())
	assert.True(t, resp.IsDir())
	assert.EqualValues(t, 0, resp.Size(), 0)
	assert.EqualValues(t, time.Now().Year(), resp.ModTime().Year())
	assert.EqualValues(t, time.Now().Month(), resp.ModTime().Month())
}

func TestStatDirWithoutPermission(t *testing.T) {
	client2 := getClientForUser(t, "gohdfs2")

	mkdirpMask(t, "/_test/accessdenied", 0700)
	touchMask(t, "/_test/accessdenied/foo", 0600)

	resp, err := client2.Stat("/_test/accessdenied")
	assert.NoError(t, err)
	assert.NotEqual(t, "", resp.(*FileInfo).Owner())

	_, err = client2.Stat("/_test/accessdenied/foo")
	assertPathError(t, err, "stat", "/_test/accessdenied/foo", os.ErrPermission)
}
