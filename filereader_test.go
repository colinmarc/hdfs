package hdfs

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"testing"
)

const (
	// many grep -b died to bring us this information...
	testStr    = "Abominable are the tumblers into which he pours his poison."
	testStrOff = 48847

	testStr2            = "http://www.gutenberg.org"
	testStr2Off         = 1256988
	testStr2NegativeOff = -288
)

func TestFileRead(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/foo.txt")
	require.Nil(t, err)

	bytes, err := ioutil.ReadAll(file)
	assert.Nil(t, err)
	assert.Equal(t, "bar\n", string(bytes))
}

func TestFileBigRead(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/mobydick.txt")
	require.Nil(t, err)

	io.Copy(ioutil.Discard, file)
}

func TestFileReadAt(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/mobydick.txt")
	require.Nil(t, err)

	buf := make([]byte, len(testStr))
	off := 0
	for off < len(buf) {
		n, err := file.ReadAt(buf[off:], int64(testStrOff+off))
		assert.Nil(t, err)
		assert.True(t, n > 0)
		off += n
	}

	assert.Equal(t, string(buf), testStr)

	buf = make([]byte, len(testStr2))
	off = 0
	for off < len(buf) {
		n, err := file.ReadAt(buf[off:], int64(testStr2Off+off))
		assert.Nil(t, err)
		assert.True(t, n > 0)
		off += n
	}

	assert.Equal(t, testStr2, string(buf))
}

func TestFileSeek(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/mobydick.txt")
	require.Nil(t, err)

	buf := new(bytes.Buffer)

	off, err := file.Seek(testStrOff, 0)
	assert.Nil(t, err)
	assert.Equal(t, testStrOff, off)

	n, err := io.CopyN(buf, file, int64(len(testStr)))
	assert.Nil(t, err)
	assert.Equal(t, len(testStr), n)
	assert.Equal(t, testStr, string(buf.Bytes()))

	// seek backwards and read it again
	off, err = file.Seek(-1*int64(len(testStr)), 1)
	assert.Nil(t, err)
	assert.Equal(t, testStrOff, off)

	buf.Reset()
	n, err = io.CopyN(buf, file, int64(len(testStr)))
	assert.Nil(t, err)
	assert.Equal(t, len(testStr), n)
	assert.Equal(t, testStr, string(buf.Bytes()))

	// now seek forward to another block and read a string
	off, err = file.Seek(testStr2NegativeOff, 2)
	assert.Nil(t, err)
	assert.Equal(t, testStr2Off, off)

	buf.Reset()
	n, err = io.CopyN(buf, file, int64(len(testStr2)))
	assert.Nil(t, err)
	assert.Equal(t, len(testStr2), n)
	assert.Equal(t, testStr2, string(buf.Bytes()))
}

func TestFileReadDir(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/full")
	require.Nil(t, err)

	res, err := file.Readdir(2)
	require.Equal(t, 2, len(res))
	assert.Equal(t, "/full/1", res[0].Name())
	assert.Equal(t, "/full/2", res[1].Name())

	res, err = file.Readdir(5)
	require.Equal(t, 2, len(res))
	assert.Equal(t, "/full/3", res[0].Name())
	assert.Equal(t, "/full/dir", res[1].Name())

	res, err = file.Readdir(0)
	require.Equal(t, 4, len(res))
	assert.Equal(t, "/full/1", res[0].Name())
	assert.Equal(t, "/full/2", res[1].Name())
	assert.Equal(t, "/full/3", res[2].Name())
	assert.Equal(t, "/full/dir", res[3].Name())
}

func TestFileReadDirnames(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/full")
	require.Nil(t, err)

	res, err := file.Readdirnames(0)
	require.Equal(t, 4, len(res))
	assert.Equal(t, []string{"/full/1", "/full/2", "/full/3", "/full/dir"}, res)
}
