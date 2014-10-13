package hdfs

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestChmod(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/tochmod")

	err := client.Chmod("/_test/tochmod", 0777)
	assert.Nil(t, err)

	fi, err := client.Stat("/_test/tochmod")
	assert.Nil(t, err)
	assert.Equal(t, 0777, fi.Mode())
}

func TestChmodDir(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/dirtochmod")

	err := client.Chmod("/_test/dirtochmod", 0777)
	assert.Nil(t, err)

	fi, err := client.Stat("/_test/dirtochmod")
	assert.Nil(t, err)
	assert.Equal(t, 0777, fi.Mode())
}

func TestChmodNonexistent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")

	err := client.Chmod("/_test/nonexistent", 0777)
	assert.Equal(t, os.ErrNotExist, err)
}

func TestChmodWithoutPermission(t *testing.T) {
	otherClient := getClientForUser(t, "other")

	mkdirp(t, "/_test/accessdenied")

	err := otherClient.Chmod("/_test/accessdenied", 0777)
	assert.Equal(t, os.ErrPermission, err)
}

func TestChown(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/tochown")
	touch(t, "/_test/tochown")

	err := client.Chown("/_test/tochown", "other", "")
	assert.Nil(t, err)

	fi, err := client.Stat("/_test/tochown")
	assert.Nil(t, err)
	assert.Equal(t, fi.(*FileInfo).Owner(), "other")
}

func TestChownDir(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/tochowndir")
	mkdirp(t, "/_test/tochowndir")

	err := client.Chown("/_test/tochowndir", "other", "")
	assert.Nil(t, err)

	fi, err := client.Stat("/_test/tochowndir")
	assert.Nil(t, err)
	assert.Equal(t, fi.(*FileInfo).Owner(), "other")
}

func TestChownNonexistent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")

	err := client.Chown("/_test/nonexistent", "other", "")
	assert.Equal(t, os.ErrNotExist, err)
}

func TestChownWithoutPermission(t *testing.T) {
	otherClient := getClientForUser(t, "other")

	mkdirp(t, "/_test/accessdenied")

	err := otherClient.Chown("/_test/accessdenied", "owner", "")
	assert.Equal(t, os.ErrPermission, err)
}
