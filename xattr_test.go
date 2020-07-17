package hdfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListXAttrs(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/xattributes")
	touch(t, "/_test/xattributes")

	err := client.SetXAttr("/_test/xattributes", "user.foo", "baz")
	require.NoError(t, err)

	err = client.SetXAttr("/_test/xattributes", "user.bar", "qux")
	require.NoError(t, err)

	xattrs, err := client.ListXAttrs("/_test/xattributes")
	require.NoError(t, err)

	assert.Equal(t, 2, len(xattrs))
	assert.Equal(t, "baz", xattrs["user.foo"])
	assert.Equal(t, "qux", xattrs["user.bar"])
}

func TestListXAttrsNonexistent(t *testing.T) {
	client := getClient(t)

	xattrs, err := client.ListXAttrs("/_test/nonexistent")
	assertPathError(t, err, "list xattrs", "/_test/nonexistent", os.ErrNotExist)
	assert.Nil(t, xattrs)
}

func TestListXAttrsEmpty(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/emptyfile")

	xattrs, err := client.ListXAttrs("/_test/emptyfile")
	require.NoError(t, err)
	assert.Equal(t, 0, len(xattrs))
}

func TestListXAttrsDir(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/xattributesdir")
	mkdirp(t, "/_test/xattributesdir")

	err := client.SetXAttr("/_test/xattributes", "user.foo", "baz")
	require.NoError(t, err)

	err = client.SetXAttr("/_test/xattributes", "user.bar", "qux")
	require.NoError(t, err)

	xattrs, err := client.ListXAttrs("/_test/xattributes")
	require.NoError(t, err)

	assert.Equal(t, 2, len(xattrs))
	assert.Equal(t, "baz", xattrs["user.foo"])
	assert.Equal(t, "qux", xattrs["user.bar"])
}

func TestListXAttrsWithoutPermission(t *testing.T) {
	// HDP 2.6.x doesn't seem to throw an error for this one.
	t.Skip()

	client2 := getClientForUser(t, "gohdfs2")

	baleet(t, "/_test/accessdenied")
	touchMask(t, "/_test/accessdenied", 0700)

	_, err := client2.ListXAttrs("/_test/accessdenied")
	assertPathError(t, err, "list xattrs", "/_test/accessdenied", os.ErrPermission)
}

func TestGetXAttrs(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/xattributes")
	touch(t, "/_test/xattributes")

	err := client.SetXAttr("/_test/xattributes", "user.foo", "baz")
	require.NoError(t, err)

	err = client.SetXAttr("/_test/xattributes", "user.bar", "qux")
	require.NoError(t, err)

	xattrs, err := client.GetXAttrs("/_test/xattributes", "user.foo", "user.bar")
	require.NoError(t, err)

	assert.Equal(t, 2, len(xattrs))
	assert.Equal(t, "baz", xattrs["user.foo"])
	assert.Equal(t, "qux", xattrs["user.bar"])
}

func TestGetXAttrsNonexistent(t *testing.T) {
	client := getClient(t)

	xattrs, err := client.GetXAttrs("/_test/nonexistent", "user.foo")
	assertPathError(t, err, "get xattrs", "/_test/nonexistent", os.ErrNotExist)
	assert.Nil(t, xattrs)
}

func TestGetXAttrsEmpty(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/xattributes")
	touch(t, "/_test/xattributes")

	xattrs, err := client.GetXAttrs("/_test/xattributes")
	require.NoError(t, err)
	assert.Equal(t, 0, len(xattrs))
}

func TestGetXAttrsNoKeys(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/xattributes")
	touch(t, "/_test/xattributes")

	err := client.SetXAttr("/_test/xattributes", "user.foo", "bar")
	require.NoError(t, err)

	xattrs, err := client.GetXAttrs("/_test/xattributes")
	require.NoError(t, err)
	assert.Equal(t, 0, len(xattrs))
}

func TestGetXAttrsNonexistentKeys(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/xattributes")
	touch(t, "/_test/xattributes")

	err := client.SetXAttr("/_test/xattributes", "user.foo", "bar")
	require.NoError(t, err)

	xattrs, err := client.GetXAttrs("/_test/xattributes", "user.baz")
	assertPathError(t, err, "get xattrs", "/_test/xattributes", errXAttrKeysNotFound)
	assert.Nil(t, xattrs)
}

func TestGetXAttrsWithoutPermission(t *testing.T) {
	client2 := getClientForUser(t, "gohdfs2")

	baleet(t, "/_test/accessdenied")
	mkdirpMask(t, "/_test/accessdenied", 0700)

	_, err := client2.GetXAttrs("/_test/accessdenied", "user.foo")
	assertPathError(t, err, "get xattrs", "/_test/accessdenied", os.ErrPermission)
}

func TestGetXAttrsTrusted(t *testing.T) {
	client := getClient(t)

	xattrs, err := client.GetXAttrs("/_test/xattributes", "trusted.foo")
	assertPathError(t, err, "get xattrs", "/_test/xattributes", os.ErrPermission)
	assert.Nil(t, xattrs)
}

func TestSetXAttrNonexistent(t *testing.T) {
	client := getClient(t)

	err := client.SetXAttr("/_test/nonexistent", "user.foo", "baz")
	assertPathError(t, err, "set xattr", "/_test/nonexistent", os.ErrNotExist)
}

func TestSetXAttrWithoutPermission(t *testing.T) {
	client2 := getClientForUser(t, "gohdfs2")

	baleet(t, "/_test/accessdenied")
	mkdirpMask(t, "/_test/accessdenied", 0700)

	err := client2.SetXAttr("/_test/accessdenied", "user.foo", "baz")
	assertPathError(t, err, "set xattr", "/_test/accessdenied", os.ErrPermission)
}

func TestSetXAttrsTrusted(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/xattributes")
	touch(t, "/_test/xattributes")

	err := client.SetXAttr("/_test/xattributes", "trusted.foo", "alert")
	assertPathError(t, err, "set xattr", "/_test/xattributes", os.ErrPermission)
}

func TestRemoveXAttrs(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/xattributes")
	touch(t, "/_test/xattributes")

	err := client.SetXAttr("/_test/xattributes", "user.foo", "baz")
	require.NoError(t, err)

	err = client.SetXAttr("/_test/xattributes", "user.bar", "qux")
	require.NoError(t, err)

	xattrs, err := client.ListXAttrs("/_test/xattributes")
	require.NoError(t, err)

	assert.Equal(t, 2, len(xattrs))
	assert.Equal(t, "baz", xattrs["user.foo"])
	assert.Equal(t, "qux", xattrs["user.bar"])

	err = client.RemoveXAttr("/_test/xattributes", "user.bar")
	require.NoError(t, err)

	xattrs, err = client.ListXAttrs("/_test/xattributes")
	require.NoError(t, err)

	assert.Equal(t, 1, len(xattrs))
	assert.Equal(t, "baz", xattrs["user.foo"])
}

func TestRemoveXAttrsNonexistent(t *testing.T) {
	client := getClient(t)

	err := client.RemoveXAttr("/_test/nonexistent", "user.foo")
	assertPathError(t, err, "remove xattr", "/_test/nonexistent", os.ErrNotExist)
}

func TestRemoveXAttrsEmpty(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/xattributes")
	touch(t, "/_test/xattributes")

	err := client.RemoveXAttr("/_test/xattributes", "user.foo")
	assertPathError(t, err, "remove xattr", "/_test/xattributes", errXAttrKeysNotFound)
}

func TestRemoveXAttrsNoMatchingKey(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/xattributes")
	touch(t, "/_test/xattributes")

	err := client.SetXAttr("/_test/xattributes", "user.foo", "baz")
	require.NoError(t, err)

	err = client.RemoveXAttr("/_test/xattributes", "user.bar")
	assertPathError(t, err, "remove xattr", "/_test/xattributes", errXAttrKeysNotFound)
}

func TestRemoveXAttrsWithoutPermission(t *testing.T) {
	client := getClient(t)
	client2 := getClientForUser(t, "gohdfs2")

	baleet(t, "/_test/accessdenied")
	mkdirpMask(t, "/_test/accessdenied", 0700)

	err := client.SetXAttr("/_test/xattributes", "user.foo", "baz")
	require.NoError(t, err)

	err = client2.RemoveXAttr("/_test/accessdenied", "user.foo")
	assertPathError(t, err, "remove xattr", "/_test/accessdenied", os.ErrPermission)
}

func TestRemoveXAttrsTrusted(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/xattributes")
	touch(t, "/_test/xattributes")

	err := client.RemoveXAttr("/_test/xattributes", "trusted.foo")
	assertPathError(t, err, "remove xattr", "/_test/xattributes", os.ErrPermission)
}
