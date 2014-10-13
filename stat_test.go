package hdfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestStat(t *testing.T) {
	client := getClient(t)

	resp, err := client.Stat("/_test/foo.txt")
	require.Nil(t, err)

	assert.Equal(t, "/_test/foo.txt", resp.Name())
	assert.False(t, resp.IsDir())
	assert.Equal(t, 4, resp.Size())
	assert.Equal(t, time.Now().Year(), resp.ModTime().Year())
	assert.Equal(t, time.Now().Month(), resp.ModTime().Month())
}

func TestStatEmptyFile(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/emptyfile2")

	resp, err := client.Stat("/_test/emptyfile2")
	require.Nil(t, err)

	assert.Equal(t, "/_test/emptyfile2", resp.Name())
	assert.False(t, resp.IsDir())
	assert.Equal(t, 0, resp.Size())
	assert.Equal(t, time.Now().Year(), resp.ModTime().Year())
	assert.Equal(t, time.Now().Month(), resp.ModTime().Month())
}

func TestStatNotExistent(t *testing.T) {
	client := getClient(t)

	resp, err := client.Stat("/_test/nonexistent")
	assert.Equal(t, os.ErrNotExist, err)
	assert.Nil(t, resp)
}

func TestStatDir(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/dir")

	resp, err := client.Stat("/_test/dir")
	require.Nil(t, err)

	assert.Equal(t, "/_test/dir", resp.Name())
	assert.True(t, resp.IsDir())
	assert.Equal(t, 0, resp.Size(), 0)
	assert.Equal(t, time.Now().Year(), resp.ModTime().Year())
	assert.Equal(t, time.Now().Month(), resp.ModTime().Month())
}

func TestStatDirWithoutPermission(t *testing.T) {
	otherClient := getClientForUser(t, "other")

	mkdirp(t, "/_test/accessdenied")
	touch(t, "/_test/accessdenied/foo")

	resp, err := otherClient.Stat("/_test/accessdenied")
	assert.Nil(t, err)
	assert.NotEqual(t, "", resp.(*FileInfo).Owner())

	_, err = otherClient.Stat("/_test/accessdenied/foo")
	assert.Equal(t, os.ErrPermission, err)
}
