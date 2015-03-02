package rpc

import (
	"code.google.com/p/goprotobuf/proto"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"testing"
	"testing/iotest"
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

func getBlocks(t *testing.T, name string) []*hdfs.LocatedBlockProto {
	namenode := getNamenode(t)

	req := &hdfs.GetBlockLocationsRequestProto{
		Src:    proto.String(name),
		Offset: proto.Uint64(0),
		Length: proto.Uint64(1257276),
	}
	resp := &hdfs.GetBlockLocationsResponseProto{}

	err := namenode.Execute("getBlockLocations", req, resp)
	if err != nil {
		t.Fatal(err)
	}

	// add a duplicate location to failover to
	return resp.GetLocations().GetBlocks()
}

func setupFailover(t *testing.T) *BlockReader {
	block := getBlocks(t, "/_test/mobydick.txt")[0]
	block.Locs = append(block.GetLocs(), block.GetLocs()...)

	br := NewBlockReader(block, 0)
	err := br.connectNext()
	if err != nil {
		t.Fatal(err)
	}

	return br
}

func TestFailsOver(t *testing.T) {
	br := setupFailover(t)
	dn := br.datanodes.datanodes[0]
	br.stream.reader = iotest.TimeoutReader(br.stream.reader)

	hash := crc32.NewIEEE()
	n, err := io.Copy(hash, br)
	require.NoError(t, err)
	assert.Equal(t, 1048576, n)
	assert.EqualValues(t, 0xb35a6a0e, hash.Sum32())
	assert.Equal(t, 0, br.datanodes.numRemaining())

	_, exist := datanodeFailures[dn]
	assert.True(t, exist)
}

func TestFailsOverMidRead(t *testing.T) {
	br := setupFailover(t)
	dn := br.datanodes.datanodes[0]

	hash := crc32.NewIEEE()
	_, err := io.CopyN(hash, br, 10000)
	require.NoError(t, err)

	br.stream.reader = iotest.TimeoutReader(br.stream.reader)

	n, err := io.Copy(hash, br)
	require.NoError(t, err)
	assert.Equal(t, 1048576-10000, n)
	assert.EqualValues(t, 0xb35a6a0e, hash.Sum32())
	assert.Equal(t, 0, br.datanodes.numRemaining())

	_, exist := datanodeFailures[dn]
	assert.True(t, exist)
}

func TestFailsOverAndThenDies(t *testing.T) {
	br := setupFailover(t)

	br.stream.reader = iotest.TimeoutReader(br.stream.reader)

	_, err := io.CopyN(ioutil.Discard, br, 10000)
	require.NoError(t, err)
	assert.Equal(t, 0, br.datanodes.numRemaining())

	br.stream.reader = iotest.TimeoutReader(br.stream.reader)
	_, err = io.Copy(ioutil.Discard, br)
	assert.Equal(t, iotest.ErrTimeout, err)
}
