package hdfs

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestMkdir(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/dir2")

	err := client.Mkdir("/_test/dir2", 777)
	assert.Nil(t, err)

	fi, err := client.Stat("/_test/dir2")
	assert.Nil(t, err)
	assert.True(t, fi.IsDir())
}

func TestMkdirExists(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/existingdir")

	err := client.Mkdir("/_test/existingdir", 777)
	assert.Equal(t, os.ErrExist, err)
}

func TestMkdirWithoutParent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")

	err := client.Mkdir("/_test/nonexistent/foo", 777)
	assert.Equal(t, os.ErrNotExist, err)

	_, err = client.Stat("/_test/nonexistent/foo")
	assert.Equal(t, os.ErrNotExist, err)

	_, err = client.Stat("/_test/nonexistent")
	assert.Equal(t, os.ErrNotExist, err)
}

func TestMkdirAll(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/dir3")

	err := client.MkdirAll("/_test/dir3/foo", 777)
	assert.Nil(t, err)

	fi, err := client.Stat("/_test/dir3/foo")
	assert.Nil(t, err)
	assert.True(t, fi.IsDir())

	fi, err = client.Stat("/_test/dir3")
	assert.Nil(t, err)
	assert.True(t, fi.IsDir())
	assert.Equal(t, 0, fi.Size())
}
