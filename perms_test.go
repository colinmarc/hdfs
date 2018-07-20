package hdfs

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChmod(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/tochmod")

	err := client.Chmod("/_test/tochmod", 0777)
	require.NoError(t, err)

	fi, err := client.Stat("/_test/tochmod")
	assert.NoError(t, err)
	assert.EqualValues(t, 0777, fi.Mode())
}

func TestChmodDir(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/dirtochmod")

	err := client.Chmod("/_test/dirtochmod", 0777)
	require.NoError(t, err)

	fi, err := client.Stat("/_test/dirtochmod")
	assert.NoError(t, err)
	assert.EqualValues(t, 0777|os.ModeDir, fi.Mode())
}

func TestChmodNonexistent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")

	err := client.Chmod("/_test/nonexistent", 0777)
	assertPathError(t, err, "chmod", "/_test/nonexistent", os.ErrNotExist)
}

func TestChmodWithoutPermission(t *testing.T) {
	client2 := getClientForUser(t, "gohdfs2")

	mkdirp(t, "/_test/accessdenied")

	err := client2.Chmod("/_test/accessdenied", 0777)
	assertPathError(t, err, "chmod", "/_test/accessdenied", os.ErrPermission)
}

func TestChown(t *testing.T) {
	superClient := getClientForSuperUser(t)

	touch(t, "/_test/tochown")

	err := superClient.Chown("/_test/tochown", "foo", "bar")
	require.NoError(t, err)

	fi, err := superClient.Stat("/_test/tochown")
	assert.NoError(t, err)
	assert.EqualValues(t, fi.(*FileInfo).Owner(), "foo")
	assert.EqualValues(t, fi.(*FileInfo).OwnerGroup(), "bar")
}

func TestChownDir(t *testing.T) {
	superClient := getClientForSuperUser(t)

	mkdirp(t, "/_test/tochowndir")

	err := superClient.Chown("/_test/tochowndir", "foo", "bar")
	require.NoError(t, err)

	fi, err := superClient.Stat("/_test/tochowndir")
	assert.NoError(t, err)
	assert.EqualValues(t, fi.(*FileInfo).Owner(), "foo")
	assert.EqualValues(t, fi.(*FileInfo).OwnerGroup(), "bar")
}

func TestChownNonexistent(t *testing.T) {
	superClient := getClientForSuperUser(t)

	baleet(t, "/_test/nonexistent")

	err := superClient.Chown("/_test/nonexistent", "gohdfs2", "")
	assertPathError(t, err, "chown", "/_test/nonexistent", os.ErrNotExist)
}

func TestChownWithoutPermission(t *testing.T) {
	client2 := getClientForUser(t, "gohdfs2")

	mkdirp(t, "/_test/accessdenied")

	err := client2.Chown("/_test/accessdenied", "owner", "")
	assertPathError(t, err, "chown", "/_test/accessdenied", os.ErrPermission)
}

func TestChtimes(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/tochtime")

	birthday := time.Date(1990, 1, 22, 14, 33, 35, 0, time.UTC)
	client.Chtimes("/_test/tochtime", birthday, birthday)

	fi, err := client.Stat("/_test/tochtime")
	assert.NoError(t, err)
	assert.EqualValues(t, birthday, fi.ModTime().UTC(), birthday)
	assert.EqualValues(t, birthday, fi.(*FileInfo).AccessTime().UTC(), birthday)
}
