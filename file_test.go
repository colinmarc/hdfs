package hdfs

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestFileRead(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/foo")
	assert.Nil(t, err)

	bytes, err := ioutil.ReadAll(file)
	assert.Nil(t, err)
	assert.Equal(t, bytes, []byte("bar"))
}
