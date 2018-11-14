package hdfs

import (
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveAndRemoveAll(t *testing.T) {
	funcs := map[string]func(*Client, string) error{
		"Remove": func(c *Client, name string) error {
			return c.Remove(name)
		},
		"RemoveAll": func(c *Client, name string) error {
			return c.RemoveAll(name)
		},
	}

	for fnName, fn := range funcs {
		t.Run("Test"+fnName+"File", func(t *testing.T) {
			client := getClient(t)

			baleet(t, "/_test/todelete")
			mkdirp(t, "/_test/todelete")
			touch(t, "/_test/todelete/deleteme")

			err := fn(client, "/_test/todelete/deleteme")
			require.NoError(t, err)

			fi, err := client.Stat("/_test/todelete/deleteme")
			assert.Nil(t, fi)
			assertPathError(t, err, "stat", "/_test/todelete/deleteme", os.ErrNotExist)
		})

		t.Run("Test"+fnName+"EmptyDir", func(t *testing.T) {
			client := getClient(t)

			baleet(t, "/_test/todelete")
			mkdirp(t, "/_test/todelete")

			err := fn(client, "/_test/todelete")
			require.NoError(t, err)

			fi, err := client.Stat("/_test/todelete")
			assert.Nil(t, fi)
			assertPathError(t, err, "stat", "/_test/todelete", os.ErrNotExist)
		})

		t.Run("Test"+fnName+"NotExistent", func(t *testing.T) {
			client := getClient(t)
			baleet(t, "/_test/nonexistent")

			err := fn(client, "/_test/nonexistent")
			assertPathError(t, err, "remove", "/_test/nonexistent", os.ErrNotExist)
		})

		t.Run("Test"+fnName+"WithoutPermission", func(t *testing.T) {
			client := getClientForUser(t, "gohdfs2")

			mkdirp(t, "/_test/accessdenied")
			touch(t, "/_test/accessdenied/foo")

			err := fn(client, "/_test/accessdenied/foo")
			assertPathError(t, err, "remove", "/_test/accessdenied/foo", os.ErrPermission)
		})
	}
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
