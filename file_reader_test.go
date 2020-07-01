package hdfs

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// many grep -b died to bring us this information...
	testStr    = "Abominable are the tumblers into which he pours his poison."
	testStrOff = 48847

	testStr2            = "http://www.gutenberg.org"
	testStr2Off         = 1256988
	testStr2NegativeOff = -288

	testChecksum = "27c076e4987344253650d3335a5d08ce"
)

type randomReadConn struct {
	net.Conn
}

func (r *randomReadConn) Read(b []byte) (int, error) {
	return r.Conn.Read(b[0:rand.Intn(len(b)+1)])
}

func TestFileRead(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/foo.txt")
	require.NoError(t, err)

	bytes, err := ioutil.ReadAll(file)
	assert.NoError(t, err)
	assert.EqualValues(t, "bar\n", string(bytes))

	info := file.Stat()
	assert.False(t, info.IsDir())
	assert.EqualValues(t, 4, info.Size())
	assert.EqualValues(t, time.Now().Year(), info.ModTime().Year())
	assert.EqualValues(t, time.Now().Month(), info.ModTime().Month())
}

func TestReadEmptyFile(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/emptyfile")

	file, err := client.Open("/_test/emptyfile")
	require.NoError(t, err)

	bytes, err := ioutil.ReadAll(file)
	assert.NoError(t, err)
	assert.EqualValues(t, "", string(bytes))
}

func TestFileBigRead(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/mobydick.txt")
	require.NoError(t, err)

	hash := crc32.NewIEEE()
	n, err := io.Copy(hash, file)
	assert.NoError(t, err)
	assert.EqualValues(t, n, 1257276)
	assert.EqualValues(t, 0x199d1ae6, hash.Sum32())
}

func TestFileBigReadWeirdSizes(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/mobydick.txt")
	require.NoError(t, err)

	hash := crc32.NewIEEE()
	copied := 0
	var n int64
	for err == nil {
		n, err = io.CopyN(hash, file, int64(rand.Intn(1000)))
		copied += int(n)
	}

	assert.EqualValues(t, io.EOF, err)
	assert.EqualValues(t, 0x199d1ae6, hash.Sum32())
	assert.EqualValues(t, copied, 1257276)
}

func TestFileBigReadWeirdSizesMisalignment(t *testing.T) {
	client := getClient(t)
	dial := client.options.DatanodeDialFunc
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}

	client.options.DatanodeDialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}

		return &randomReadConn{conn}, nil
	}

	file, err := client.Open("/_test/mobydick.txt")
	require.NoError(t, err)

	hash := crc32.NewIEEE()
	copied := 0
	var n int64
	for err == nil {
		n, err = io.CopyN(hash, file, int64(rand.Intn(1000)))
		copied += int(n)
	}

	assert.EqualValues(t, io.EOF, err)
	assert.EqualValues(t, 0x199d1ae6, hash.Sum32())
	assert.EqualValues(t, copied, 1257276)
}

func TestFileBigReadN(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/mobydick.txt")
	require.NoError(t, err)

	n, err := io.CopyN(ioutil.Discard, file, 1000000)
	assert.NoError(t, err)
	assert.EqualValues(t, n, 1000000)
}

func TestFileReadNil(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/mobydick.txt")
	require.NoError(t, err)

	n, err := file.Read(nil)
	assert.NoError(t, err)
	assert.EqualValues(t, n, 0)
}

func TestFileReadAt(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/mobydick.txt")
	require.NoError(t, err)

	buf := make([]byte, len(testStr))
	off := 0
	for off < len(buf) {
		n, err := file.ReadAt(buf[off:], int64(testStrOff+off))
		require.NoError(t, err)
		assert.True(t, n > 0)
		off += n
	}

	assert.EqualValues(t, string(buf), testStr)

	buf = make([]byte, len(testStr2))
	off = 0
	for off < len(buf) {
		n, err := file.ReadAt(buf[off:], int64(testStr2Off+off))
		require.NoError(t, err)
		assert.True(t, n > 0)
		off += n
	}

	assert.EqualValues(t, testStr2, string(buf))
}

func TestFileReadAtEOF(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/foo.txt")
	require.NoError(t, err)

	buf := make([]byte, 10)
	_, err = file.ReadAt(buf, 1)

	assert.Equal(t, append([]byte{'a', 'r', '\n'}, make([]byte, 7)...), buf)
	assert.Equal(t, io.EOF, err)
}

func TestFileReadOversizedBuffer(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/foo.txt")
	require.NoError(t, err)

	buf := make([]byte, 1025)
	n, err := file.Read(buf)

	assert.Equal(t, 4, n)
	assert.Equal(t, append([]byte{'b', 'a', 'r', '\n'}, make([]byte, 1025-4)...), buf)
	if err != io.EOF {
		assert.NoError(t, err)
	}
}

func TestFileSeek(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/mobydick.txt")
	require.NoError(t, err)

	buf := new(bytes.Buffer)

	off, err := file.Seek(testStrOff, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, testStrOff, off)

	n, err := io.CopyN(buf, file, int64(len(testStr)))
	assert.NoError(t, err)
	assert.EqualValues(t, len(testStr), n)
	assert.EqualValues(t, testStr, string(buf.Bytes()))

	// seek backwards and read it again
	off, err = file.Seek(-1*int64(len(testStr)), 1)
	assert.NoError(t, err)
	assert.EqualValues(t, testStrOff, off)

	buf.Reset()
	n, err = io.CopyN(buf, file, int64(len(testStr)))
	assert.NoError(t, err)
	assert.EqualValues(t, len(testStr), n)
	assert.EqualValues(t, testStr, string(buf.Bytes()))

	// now seek forward to another block and read a string
	off, err = file.Seek(testStr2NegativeOff, 2)
	assert.NoError(t, err)
	assert.EqualValues(t, testStr2Off, off)

	buf.Reset()
	n, err = io.CopyN(buf, file, int64(len(testStr2)))
	assert.NoError(t, err)
	assert.EqualValues(t, len(testStr2), n)
	assert.EqualValues(t, testStr2, string(buf.Bytes()))
}

func TestFileReadDir(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/fulldir3")
	mkdirp(t, "/_test/fulldir3/dir")
	touch(t, "/_test/fulldir3/1")
	touch(t, "/_test/fulldir3/2")
	touch(t, "/_test/fulldir3/3")

	file, err := client.Open("/_test/fulldir3")
	require.NoError(t, err)

	res, err := file.Readdir(2)
	require.Equal(t, 2, len(res))
	assert.EqualValues(t, "1", res[0].Name())
	assert.EqualValues(t, "2", res[1].Name())

	res, err = file.Readdir(5)
	require.Equal(t, 2, len(res))
	assert.EqualValues(t, "3", res[0].Name())
	assert.EqualValues(t, "dir", res[1].Name())

	res, err = file.Readdir(0)
	require.Equal(t, 4, len(res))
	assert.EqualValues(t, "1", res[0].Name())
	assert.EqualValues(t, "2", res[1].Name())
	assert.EqualValues(t, "3", res[2].Name())
	assert.EqualValues(t, "dir", res[3].Name())
}

func TestFileReadDirnames(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/fulldir4")
	mkdirp(t, "/_test/fulldir4/dir")
	touch(t, "/_test/fulldir4/1")
	touch(t, "/_test/fulldir4/2")
	touch(t, "/_test/fulldir4/3")

	file, err := client.Open("/_test/fulldir4")
	require.NoError(t, err)

	res, err := file.Readdirnames(0)
	require.Equal(t, 4, len(res))
	assert.EqualValues(t, []string{"1", "2", "3", "dir"}, res)
}

func TestFileReadDirMany(t *testing.T) {
	client := getClient(t)

	maxReadDir := 1000 // HDFS returns this many entries.
	total := maxReadDir*2 + maxReadDir/2 + 35
	firstBatch := maxReadDir + 71

	mkdirp(t, "/_test/fulldir5")
	for i := 0; i < total; i++ {
		touch(t, fmt.Sprintf("/_test/fulldir5/%04d", i))
	}

	file, err := client.Open("/_test/fulldir5")
	require.NoError(t, err)

	res, err := file.Readdir(firstBatch)
	require.Equal(t, firstBatch, len(res))
	for i := range res {
		assert.EqualValues(t, fmt.Sprintf("%04d", i), res[i].Name())
	}

	res, err = file.Readdir(total)
	assert.Equal(t, total-firstBatch, len(res))
	for i := range res {
		assert.EqualValues(t, fmt.Sprintf("%04d", i+firstBatch), res[i].Name())
	}

	res, err = file.Readdir(0)
	require.Equal(t, total, len(res))
	for i := range res {
		assert.EqualValues(t, fmt.Sprintf("%04d", i), res[i].Name())
	}
}

func TestOpenFileWithoutPermission(t *testing.T) {
	client2 := getClientForUser(t, "gohdfs2")

	mkdirpMask(t, "/_test/accessdenied", 0700)
	touchMask(t, "/_test/accessdenied/foo", 0700)

	file, err := client2.Open("/_test/accessdenied/foo")
	assertPathError(t, err, "open", "/_test/accessdenied/foo", os.ErrPermission)
	assert.Nil(t, file)
}

func TestFileChecksum(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/foo.txt")
	require.NoError(t, err)

	checksum, err := file.Checksum()
	require.NoError(t, err)

	assert.EqualValues(t, testChecksum, hex.EncodeToString(checksum))
}

func TestFileReadDeadline(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/foo.txt")
	require.NoError(t, err)

	file.SetDeadline(time.Now().Add(200 * time.Millisecond))
	_, err = file.Read([]byte{0, 0})
	assert.NoError(t, err)

	time.Sleep(200 * time.Millisecond)
	_, err = file.Read([]byte{0, 0})
	assert.NotNil(t, err)
}

func TestFileReadDeadlineBefore(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/foo.txt")
	require.NoError(t, err)

	file.SetDeadline(time.Now())
	_, err = file.Read([]byte{0, 0})
	assert.NotNil(t, err)
}

func TestFileChecksumDeadline(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/foo.txt")
	require.NoError(t, err)

	file.SetDeadline(time.Now().Add(100 * time.Millisecond))
	_, err = file.Checksum()
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	_, err = file.Checksum()
	assert.NotNil(t, err)
}

func TestFileChecksumDeadlineBefore(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/foo.txt")
	require.NoError(t, err)

	file.SetDeadline(time.Now())
	_, err = file.Checksum()
	assert.NotNil(t, err)
}
