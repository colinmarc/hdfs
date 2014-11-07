package rpc

import (
	"code.google.com/p/goprotobuf/proto"
	"testing"
	"testing/iotest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"os"
	"os/user"
	"bufio"
	"io"
	"io/ioutil"
	"hash/crc32"
	"time"
)

func getNamenode(t *testing.T) *NamenodeConnection {
	nn := os.Getenv("HADOOP_NAMENODE")
	if nn == "" {
		t.Fatal("HADOOP_NAMENODE not set")
	}

	currentUser, _ := user.Current()
	conn, err := NewNamenodeConnection(nn, currentUser.Username)
	if err != nil {
		t.Fatal(err)
	}

	return conn
}

func setupFailover(t *testing.T) *BlockReader {
	namenode := getNamenode(t)

	req := &hdfs.GetBlockLocationsRequestProto{
		Src:    proto.String("/_test/mobydick.txt"),
		Offset: proto.Uint64(0),
		Length: proto.Uint64(1257276),
	}
	resp := &hdfs.GetBlockLocationsResponseProto{}

	err := namenode.Execute("getBlockLocations", req, resp)
	if err != nil {
		t.Fatal(err)
	}

	// add a duplicate location to failover to
	block := resp.GetLocations().GetBlocks()[0]
	block.Locs = append(block.GetLocs(), block.GetLocs()...)

	br := NewBlockReader(block, 0)
	err = br.connectNext()
	if err != nil {
		t.Fatal(err)
	}

	return br
}

func TestPicksFirstDatanode(t *testing.T) {
	br := setupFailover(t)
	br.datanodes = []string{"foo:6000", "bar:6000"}
	assert.Equal(t, br.nextDatanode(), "foo:6000")
}

func TestPicksDatanodesWithoutFailures(t *testing.T) {
	br := setupFailover(t)
	br.datanodes = []string{"foo:6000", "foo:7000", "bar:6000"}
	datanodeFailures["foo:6000"] = time.Now()

	assert.Equal(t, br.nextDatanode(), "foo:7000")
}

func TestPicksDatanodesWithOldestFailures(t *testing.T) {
	br := setupFailover(t)
	br.datanodes = []string{"foo:6000", "bar:6000"}
	datanodeFailures["foo:6000"] = time.Now().Add(-10 * time.Minute)
	datanodeFailures["bar:6000"] = time.Now()

	assert.Equal(t, br.nextDatanode(), "foo:6000")
}

func TestFailsOver(t *testing.T) {
	br := setupFailover(t)
	dn := br.datanodes[0]
	br.stream.reader = bufio.NewReaderSize(iotest.TimeoutReader(br.stream.reader), 0)

	hash := crc32.NewIEEE()
	n, err := io.Copy(hash, br)
	require.Nil(t, err)
	assert.Equal(t, 1048576, n)
	assert.Equal(t, 0x2ac4f588, hash.Sum32())
	assert.Equal(t, 0, len(br.datanodes))

	_, exist := datanodeFailures[dn]
	assert.True(t, exist)
}

func TestFailsOverAndThenDies(t *testing.T) {
	br := setupFailover(t)

	br.stream.reader = bufio.NewReaderSize(iotest.TimeoutReader(br.stream.reader), 0)

	_, err := io.CopyN(ioutil.Discard, br, 10000)
	require.Nil(t, err)
	assert.Equal(t, 0, len(br.datanodes))

	br.stream.reader = bufio.NewReaderSize(iotest.TimeoutReader(br.stream.reader), 0)
	_, err = io.Copy(ioutil.Discard, br)
	assert.Equal(t, iotest.ErrTimeout, err)
}

