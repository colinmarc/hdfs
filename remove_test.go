package hdfs

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestRemove(t *testing.T) {
	client := getClient(t)

	err := client.Remove("/todelete")
	assert.Nil(t, err)

	fi, err := client.Stat("/todelete")
	assert.Nil(t, fi)
	assert.Equal(t, os.ErrNotExist, err)
}

func TestRemoveNotExistent(t *testing.T) {
	client := getClient(t)

	err := client.Remove("/nonexistent")
	assert.Equal(t, os.ErrNotExist, err)
}
