package gohdfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuotaUsageDir(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/dirforcs")
	mkdirp(t, "/_test/dirforcs/1")
	mkdirp(t, "/_test/dirforcs/2")
	touch(t, "/_test/dirforcs/foo")
	touch(t, "/_test/dirforcs/1/bar")

	resp, err := client.GetQuotaUsage("/_test/dirforcs")
	require.NoError(t, err)

	assert.EqualValues(t, 5, resp.FileAndDirectoryCount())
	assert.True(t, resp.Quota() < 0)
	assert.True(t, resp.SpaceQuota() < 0)
	assert.True(t, resp.SpaceConsumed() == 0)
}

func TestQuotaUsageFile(t *testing.T) {
	client := getClient(t)

	resp, err := client.GetQuotaUsage("/_test/foo.txt")
	require.NoError(t, err)

	assert.EqualValues(t, 1, resp.FileAndDirectoryCount())
	assert.True(t, resp.Quota() < 0)
	assert.True(t, resp.SpaceQuota() < 0)
	assert.True(t, resp.SpaceConsumed() > 0)
}

func TestQuotaUsageNonExistent(t *testing.T) {
	client := getClient(t)

	resp, err := client.GetQuotaUsage("/_test/nonexistent")
	assertPathError(t, err, "quota usage", "/_test/nonexistent", os.ErrNotExist)
	assert.Nil(t, resp)
}

func TestQuotaUsageDirWithoutPermission(t *testing.T) {
	client2 := getClientForUser(t, "gohdfs2")

	mkdirpMask(t, "/_test/accessdenied", 0700)
	touchMask(t, "/_test/accessdenied/foo", 0600)

	_, err := client2.GetQuotaUsage("/_test/accessdenied/foo")
	assertPathError(t, err, "quota usage", "/_test/accessdenied/foo", os.ErrPermission)
}
