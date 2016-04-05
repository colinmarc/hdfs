package hdfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobFindWildcard(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/glob1")
	mkdirp(t, "/_test/glob1/dir")
	touch(t, "/_test/glob1/1")
	touch(t, "/_test/glob1/2")
	touch(t, "/_test/glob1/3")

	res, err := client.GlobFind("/_test/glob1/*")
	require.NoError(t, err)
	require.Equal(t, 4, len(res))

	assert.EqualValues(t, "1", res[0].Name())
	assert.False(t, res[0].IsDir())
}

func TestGlobFindBraces(t *testing.T) {

	client := getClient(t)

	mkdirp(t, "/_test/glob2")
	mkdirp(t, "/_test/glob2/dir")
	touch(t, "/_test/glob2/1")
	touch(t, "/_test/glob2/2")
	touch(t, "/_test/glob2/3")

	res, err := client.GlobFind("/_test/glob2/{1,2,10}")
	require.NoError(t, err)
	require.Equal(t, 2, len(res))

	assert.EqualValues(t, "1", res[0].Name())
	assert.False(t, res[0].IsDir())
}

func TestGlobFindBracesTwice(t *testing.T) {

	client := getClient(t)

	mkdirp(t, "/_test/glob3")
	mkdirp(t, "/_test/glob32")
	mkdirp(t, "/_test/emptydir")
	mkdirp(t, "/_test/glob3/dir")
	touch(t, "/_test/glob3/1")
	touch(t, "/_test/glob3/2")
	touch(t, "/_test/glob3/3")
	touch(t, "/_test/glob32/1")
	touch(t, "/_test/glob32/2")

	res, err := client.GlobFind("/_test/{glob3,glob32}/{1,2,10}")
	require.NoError(t, err)
	require.Equal(t, 4, len(res))
}

func TestGlobFindRegexWildcard(t *testing.T) {

	client := getClient(t)

	mkdirp(t, "/_test/glob4")
	mkdirp(t, "/_test/glob42")
	mkdirp(t, "/_test/emptydir")
	mkdirp(t, "/_test/glob4/dir")
	touch(t, "/_test/glob4/1")
	touch(t, "/_test/glob4/2.txt")
	touch(t, "/_test/glob4/3")
	touch(t, "/_test/glob42/1.txt")
	touch(t, "/_test/glob42/2")

	res, err := client.GlobFind("/_test/glob*/*.txt")
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
}

