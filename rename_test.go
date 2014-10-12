package hdfs

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestRename(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/tomove")
	baleet(t, "/_test/tomovedest")

	err := client.Rename("/_test/tomove", "/_test/tomovedest")
	assert.Nil(t, err)

	fi, err := client.Stat("/_test/tomove")
	assert.Nil(t, fi)
	assert.Equal(t, os.ErrNotExist, err)

	fi, err = client.Stat("/_test/tomovedest")
	assert.Nil(t, err)
}

func TestRenameSrcNotExistent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")
	baleet(t, "/_test/nonexistent2")

	err := client.Rename("/_test/nonexistent", "/_test/nonexistent2")
	assert.Equal(t, os.ErrNotExist, err)
}

func TestRenameDestExists(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/tomove2")
	touch(t, "/_test/tomovedest2")

	err := client.Rename("/_test/tomove2", "/_test/tomovedest2")
	assert.Equal(t, os.ErrExist, err)
}
