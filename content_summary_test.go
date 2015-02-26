package hdfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestContentSummaryDir(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/dirforcs/1")
	mkdirp(t, "/_test/dirforcs/2")
	touch(t, "/_test/dirforcs/foo")
	touch(t, "/_test/dirforcs/1/bar")

	resp, err := client.GetContentSummary("/_test/dirforcs")
	require.Nil(t, err)

	assert.Equal(t, 2, resp.FileCount())
	assert.Equal(t, 3, resp.DirectoryCount())
}

func TestContentSummaryFile(t *testing.T) {
	client := getClient(t)

	resp, err := client.GetContentSummary("/_test/foo.txt")
	require.Nil(t, err)

	assert.Equal(t, 4, resp.Size())
	assert.True(t, resp.SizeAfterReplication() >= 4)
	assert.Equal(t, 1, resp.FileCount())
	assert.Equal(t, 0, resp.DirectoryCount())
}

func TestContentSummaryNonExistent(t *testing.T) {
	client := getClient(t)

	resp, err := client.GetContentSummary("/_test/nonexistent")
	assertPathError(t, err, "content summary", "/_test/nonexistent", os.ErrNotExist)
	assert.Nil(t, resp)
}

func TestContentSummaryDirWithoutPermission(t *testing.T) {
	otherClient := getClientForUser(t, "other")

	mkdirp(t, "/_test/accessdenied")
	touch(t, "/_test/accessdenied/foo")

	_, err := otherClient.GetContentSummary("/_test/accessdenied/foo")
	assertPathError(t, err, "content summary", "/_test/accessdenied/foo", os.ErrPermission)
}
