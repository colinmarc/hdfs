package hdfs

import (
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"testing"
)

func TestFileRead(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/foo")
	assert.Nil(t, err)

	bytes, err := ioutil.ReadAll(file)
	assert.Nil(t, err)
	assert.Equal(t, string(bytes), "bar\n")
}

func TestFileBigRead(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/longfile")
	assert.Nil(t, err)

	io.Copy(ioutil.Discard, file)
}
