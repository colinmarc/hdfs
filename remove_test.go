package hdfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemove(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/todelete")
	mkdirp(t, "/_test/todelete")

	err := client.Remove("/_test/todelete")
	require.NoError(t, err)

	fi, err := client.Stat("/_test/todelete")
	assert.Nil(t, fi)
	assertPathError(t, err, "stat", "/_test/todelete", os.ErrNotExist)
}

func TestRemoveNotExistent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")

	err := client.Remove("/_test/nonexistent")
	assertPathError(t, err, "remove", "/_test/nonexistent", os.ErrNotExist)
}

func TestRemoveWithoutPermission(t *testing.T) {
	client2 := getClientForUser(t, "gohdfs2")

	mkdirp(t, "/_test/accessdenied")
	touch(t, "/_test/accessdenied/foo")

	err := client2.Remove("/_test/accessdenied/foo")
	assertPathError(t, err, "remove", "/_test/accessdenied/foo", os.ErrPermission)
}
