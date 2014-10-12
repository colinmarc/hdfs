package hdfs

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestRemove(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/todelete")
	mkdirp(t, "/_test/todelete")

	err := client.Remove("/_test/todelete")
	assert.Nil(t, err)

	fi, err := client.Stat("/_test/todelete")
	assert.Nil(t, fi)
	assert.Equal(t, os.ErrNotExist, err)
}

func TestRemoveNotExistent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")

	err := client.Remove("/_test/nonexistent")
	assert.Equal(t, os.ErrNotExist, err)
}
