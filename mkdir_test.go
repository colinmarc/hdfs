package hdfs

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestMkdir(t *testing.T) {
	client := getClient(t)

	err := client.Mkdir("/test", 777)
	assert.Nil(t, err)

	fi, err := client.Stat("/test")
	assert.Nil(t, err)
	assert.True(t, fi.IsDir())
	assert.Equal(t, 0, fi.Size())
}

func TestMkdirExists(t *testing.T) {
	client := getClient(t)

	err := client.Mkdir("/full", 777)
	assert.Equal(t, os.ErrExist, err)
}

func TestMkdirNested(t *testing.T) {
	client := getClient(t)

	err := client.Mkdir("/test2/foo", 777)
	assert.Equal(t, os.ErrNotExist, err)

	fi, err := client.Stat("/test2/foo")
	assert.Nil(t, fi)
	assert.Equal(t, os.ErrNotExist, err)

	fi, err = client.Stat("/test2")
	assert.Nil(t, fi)
	assert.Equal(t, os.ErrNotExist, err)
}

func TestMkdirAllNested(t *testing.T) {
	client := getClient(t)

	err := client.MkdirAll("/test3/foo", 777)
	assert.Nil(t, err)

	fi, err := client.Stat("/test3/foo")
	assert.Nil(t, err)
	assert.True(t, fi.IsDir())
	assert.Equal(t, 0, fi.Size())

	fi, err = client.Stat("/test3")
	assert.Nil(t, err)
	assert.True(t, fi.IsDir())
	assert.Equal(t, 0, fi.Size())
}
