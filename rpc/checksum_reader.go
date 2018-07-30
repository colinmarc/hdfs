package rpc

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/golang/protobuf/proto"
)

// ChecksumReader provides an interface for reading the "MD5CRC32" checksums of
// individual blocks. It abstracts over reading from multiple datanodes, in
// order to be robust to failures.
type ChecksumReader struct {
	// Block is the block location provided by the namenode.
	Block *hdfs.LocatedBlockProto
	// UseDatanodeHostname specifies whether the datanodes should be connected to
	// via their hostnames (if true) or IP addresses (if false).
	UseDatanodeHostname bool
	// DialFunc is used to connect to the datanodes. If nil, then
	// (&net.Dialer{}).DialContext is used.
	DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

	datanodes *datanodeFailover
	conn      net.Conn
	reader    *bufio.Reader
}

// NewChecksumReader creates a new ChecksumReader for the given block.
//
// Deprecated: this method does not do any required initialization, and does
// not allow you to set fields such as UseDatanodeHostname.
func NewChecksumReader(block *hdfs.LocatedBlockProto) *ChecksumReader {
	return &ChecksumReader{
		Block: block,
	}
}

// ReadChecksum returns the checksum of the block.
func (cr *ChecksumReader) ReadChecksum() ([]byte, error) {
	if cr.datanodes == nil {
		locs := cr.Block.GetLocs()
		datanodes := make([]string, len(locs))
		for i, loc := range locs {
			dn := loc.GetId()
			datanodes[i] = getDatanodeAddress(dn, cr.UseDatanodeHostname)
		}

		cr.datanodes = newDatanodeFailover(datanodes)
	}

	for cr.datanodes.numRemaining() > 0 {
		address := cr.datanodes.next()
		checksum, err := cr.readChecksum(address)
		if err != nil {
			cr.datanodes.recordFailure(err)
			continue
		}

		return checksum, nil
	}

	err := cr.datanodes.lastError()
	if err != nil {
		err = errors.New("No available datanodes for block.")
	}

	return nil, err
}

func (cr *ChecksumReader) readChecksum(address string) ([]byte, error) {
	if cr.DialFunc == nil {
		cr.DialFunc = (&net.Dialer{}).DialContext
	}

	conn, err := cr.DialFunc(context.Background(), "tcp", address)
	if err != nil {
		return nil, err
	}

	cr.conn = conn
	err = cr.writeBlockChecksumRequest()
	if err != nil {
		return nil, err
	}

	cr.reader = bufio.NewReader(conn)
	resp, err := cr.readBlockChecksumResponse()
	if err != nil {
		return nil, err
	}

	return resp.GetChecksumResponse().GetMd5(), nil
}

// A checksum request to a datanode:
// +-----------------------------------------------------------+
// |  Data Transfer Protocol Version, int16                    |
// +-----------------------------------------------------------+
// |  Op code, 1 byte (CHECKSUM_BLOCK = 0x55)                  |
// +-----------------------------------------------------------+
// |  varint length + OpReadBlockProto                         |
// +-----------------------------------------------------------+
func (cr *ChecksumReader) writeBlockChecksumRequest() error {
	header := []byte{0x00, dataTransferVersion, checksumBlockOp}

	op := newChecksumBlockOp(cr.Block)
	opBytes, err := makePrefixedMessage(op)
	if err != nil {
		return err
	}

	req := append(header, opBytes...)
	_, err = cr.conn.Write(req)
	if err != nil {
		return err
	}

	return nil
}

// The response from the datanode:
// +-----------------------------------------------------------+
// |  varint length + BlockOpResponseProto                     |
// +-----------------------------------------------------------+
func (cr *ChecksumReader) readBlockChecksumResponse() (*hdfs.BlockOpResponseProto, error) {
	respLength, err := binary.ReadUvarint(cr.reader)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}

		return nil, err
	}

	respBytes := make([]byte, respLength)
	_, err = io.ReadFull(cr.reader, respBytes)
	if err != nil {
		return nil, err
	}

	resp := &hdfs.BlockOpResponseProto{}
	err = proto.Unmarshal(respBytes, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func newChecksumBlockOp(block *hdfs.LocatedBlockProto) *hdfs.OpBlockChecksumProto {
	return &hdfs.OpBlockChecksumProto{
		Header: &hdfs.BaseHeaderProto{
			Block: block.GetB(),
			Token: block.GetBlockToken(),
		},
	}
}
