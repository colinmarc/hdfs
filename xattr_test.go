package hdfs

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
)

func TestXAttrs(t *testing.T) {
	client := getClient(t)

	err := client.SetUserAttr("/_test/foo.txt", "key", "value")
	require.NoError(t, err)

	xattrs, err := client.ListUserAttrs("/_test/foo.txt")
	require.NoError(t, err)

	assert.EqualValues(t, xattrs["key"], "value")

	err = client.RemoveUserAttr("/_test/foo.txt", "key")
	require.NoError(t, err)

	xattrs, err = client.ListUserAttrs("/_test/foo.txt")
	require.NoError(t, err)

	assert.EqualValues(t, len(xattrs), 0)
}
