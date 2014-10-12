package rpc

import (
	"bufio"
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"errors"
	"fmt"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"net"
)

const (
	dataTransferVersion = 0x1c
	readBlockOp         = 0x51
)

// TODO: datanode blacklisting

// BlockReader implements io.ReaderCloser for reading a single block from HDFS,
// abstracting over reading from multiple datanodes.
type BlockReader struct {
	closed bool
	conn   net.Conn
	reader *bufio.Reader

	block       *hdfs.LocatedBlockProto
	checksumTab *crc32.Table

	offset    uint64
	chunkSize uint32
	packet    openPacket
	buf       bytes.Buffer
}

type openPacket struct {
	numChunks     int
	nextChunk     int
	checksumBytes []byte
	blockOffset   uint64
	packetOffset  uint64
	length        uint64
	last          bool
}

// NewBlockReader returns a new BlockReader, given the block information and
// security token from the namenode.
func NewBlockReader(block *hdfs.LocatedBlockProto, offset uint64) (*BlockReader, error) {
	br := &BlockReader{
		block:  block,
		offset: offset,
	}

	// TODO check multiple datanodes
	datanode := br.block.GetLocs()[0].GetId()
	address := fmt.Sprintf("%s:%d", datanode.GetIpAddr(), datanode.GetXferPort())
	err := br.connect(address)
	if err != nil {
		return nil, err
	}

	return br, nil
}

func (br *BlockReader) connect(datanode string) error {
	conn, err := net.DialTimeout("tcp", datanode, connectionTimeout)
	if err != nil {
		return err
	}

	br.conn = conn
	err = br.writeBlockReadRequest()
	if err != nil {
		return err
	}

	br.reader = bufio.NewReader(br.conn)
	resp, err := br.readBlockReadResponse()
	if err != nil {
		return err
	}

	checksumInfo := resp.GetReadOpChecksumInfo().GetChecksum()
	checksumType := checksumInfo.GetType()
	if checksumType == hdfs.ChecksumTypeProto_CHECKSUM_CRC32 {
		br.checksumTab = crc32.IEEETable
	} else if checksumType == hdfs.ChecksumTypeProto_CHECKSUM_CRC32C {
		br.checksumTab = crc32.MakeTable(crc32.Castagnoli)
	} else {
		return fmt.Errorf("Unsupported checksum type:", checksumType)
	}

	br.chunkSize = checksumInfo.GetBytesPerChecksum()
	br.startNewPacket()

	// The read will start aligned to a chunk boundary, so we need to seek forward
	// to the requested offset.
	amountToDiscard := br.offset - br.packet.blockOffset
	if amountToDiscard > 0 {
		io.CopyN(ioutil.Discard, br, int64(amountToDiscard))
	}

	return nil
}

func (br *BlockReader) Close() {
	br.conn.Close()
	br.closed = true
}

func (br *BlockReader) Read(b []byte) (int, error) {
	if br.closed {
		return 0, errors.New("The BlockReader is closed.")
	} else if br.offset >= br.block.GetB().GetNumBytes() {
		br.Close()
		return 0, io.EOF
	}

	// first, read any leftover data from buf
	if br.buf.Len() > 0 {
		n, _ := br.buf.Read(b)
		return n, nil
	}

	if br.packet.nextChunk >= br.packet.numChunks {
		br.startNewPacket()
	}

	// then, read until we fill up b or we reach the end of the packet
	readOffset := 0
	for br.packet.nextChunk < br.packet.numChunks {
		chOff := 4 * br.packet.nextChunk
		checksum := br.packet.checksumBytes[chOff : chOff+4]

		remaining := br.packet.length - br.packet.packetOffset
		chunkLength := int64(math.Min(float64(br.chunkSize), float64(remaining)))

		chunkReader := io.LimitReader(br.reader, int64(chunkLength))
		chunkBytes := b[readOffset:]
		n, err := chunkReader.Read(chunkBytes)

		readOffset += n
		br.packet.packetOffset += uint64(n)
		br.packet.nextChunk++

		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}

			br.Close()
			return readOffset, err
		}

		crc := crc32.Checksum(chunkBytes[:n], br.checksumTab)

		if int64(n) < chunkLength {
			// save any leftovers
			br.buf.Reset()
			leftover, err := br.buf.ReadFrom(chunkReader)
			if err != nil {
				return readOffset, err
			}

			br.packet.packetOffset += uint64(leftover)

			// update the checksum with the leftovers
			crc = crc32.Update(crc, br.checksumTab, br.buf.Bytes())
		}

		if crc != binary.BigEndian.Uint32(checksum) {
			return readOffset, errors.New("Invalid checksum from the datanode!")
		}

		if readOffset == len(b) {
			break
		}
	}

	return readOffset, nil
}

// A read request to a datanode:
// +-----------------------------------------------------------+
// |  Data Transfer Protocol Version, int16                    |
// +-----------------------------------------------------------+
// |  Op code, 1 byte (READ_BLOCK = 0x51)                      |
// +-----------------------------------------------------------+
// |  varint length + OpReadBlockProto                         |
// +-----------------------------------------------------------+
func (br *BlockReader) writeBlockReadRequest() error {
	header := []byte{0x00, dataTransferVersion, readBlockOp}

	// TODO offset/length?
	needed := (br.block.GetB().GetNumBytes() - br.offset)
	op := newReadBlockOp(br.block, br.offset, needed)
	opBytes, err := makeDelimitedMsg(op)
	if err != nil {
		return err
	}

	req := append(header, opBytes...)
	_, err = br.conn.Write(req)
	if err != nil {
		return err
	}

	return nil
}

// The initial response from the datanode:
// +-----------------------------------------------------------+
// |  varint length + BlockOpResponseProto                     |
// +-----------------------------------------------------------+
func (br *BlockReader) readBlockReadResponse() (*hdfs.BlockOpResponseProto, error) {
	respLength, err := binary.ReadUvarint(br.reader)
	if err != nil {
		return nil, err
	}

	respBytes := make([]byte, respLength)
	_, err = io.ReadFull(br.reader, respBytes)
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

// A packet from the datanode:
// +-----------------------------------------------------------+
// |  uint32 length of the packet                              |
// +-----------------------------------------------------------+
// |  size of the PacketHeaderProto, uint16                    |
// +-----------------------------------------------------------+
// |  PacketHeaderProto                                        |
// +-----------------------------------------------------------+
// |  N checksums, 4 bytes each                                |
// +-----------------------------------------------------------+
// |  N chunks of payload data                                 |
// +-----------------------------------------------------------+
func (br *BlockReader) startNewPacket() error {
	header, err := br.readPacketHeader()
	if err != nil {
		return err
	}

	blockOffset := uint64(header.GetOffsetInBlock())
	dataLength := uint64(header.GetDataLen())
	numChunks := int(math.Ceil(float64(dataLength) / float64(br.chunkSize)))

	// TODO don't assume checksum size is 4
	br.packet = openPacket{
		numChunks:     numChunks,
		nextChunk:     0,
		checksumBytes: make([]byte, numChunks*4),
		blockOffset:   blockOffset,
		packetOffset:  0,
		length:        dataLength,
		last:          header.GetLastPacketInBlock(),
	}

	_, err = io.ReadFull(br.reader, br.packet.checksumBytes)
	if err != nil {
		return err
	}

	return nil
}

func (br *BlockReader) readPacketHeader() (*hdfs.PacketHeaderProto, error) {
	var packetLength uint32
	err := binary.Read(br.reader, binary.BigEndian, &packetLength)
	if err != nil {
		return nil, err
	}

	var packetHeaderLength uint16
	err = binary.Read(br.reader, binary.BigEndian, &packetHeaderLength)
	if err != nil {
		return nil, err
	}

	packetHeaderBytes := make([]byte, packetHeaderLength)
	_, err = io.ReadFull(br.reader, packetHeaderBytes)
	if err != nil {
		return nil, err
	}

	packetHeader := &hdfs.PacketHeaderProto{}
	err = proto.Unmarshal(packetHeaderBytes, packetHeader)

	return packetHeader, nil
}

func newReadBlockOp(block *hdfs.LocatedBlockProto, offset, length uint64) *hdfs.OpReadBlockProto {
	return &hdfs.OpReadBlockProto{
		Header: &hdfs.ClientOperationHeaderProto{
			BaseHeader: &hdfs.BaseHeaderProto{
				Block: block.GetB(),
				Token: block.GetBlockToken(),
			},
			ClientName: proto.String(ClientName),
		},
		Offset: proto.Uint64(offset),
		Len:    proto.Uint64(length),
	}
}
