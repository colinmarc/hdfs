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

// blockStream implements io.ReaderCloser for reading a single block from HDFS,
// from a single datanode.
type blockStream struct {
	address string
	block   *hdfs.LocatedBlockProto

	closed      bool
	conn        net.Conn
	reader      *bufio.Reader
	checksumTab *crc32.Table

	startOffset uint64
	chunkSize   uint32
	packet      openPacket
	buf         bytes.Buffer
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

// newBlockStream returns a new connected blockStream.
func newBlockStream(address string, block *hdfs.LocatedBlockProto, offset uint64) (*blockStream, error) {
	s := &blockStream{
		address:     address,
		block:       block,
		startOffset: offset,
	}

	err := s.connect()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *blockStream) connect() error {
	conn, err := net.DialTimeout("tcp", s.address, connectionTimeout)
	if err != nil {
		return err
	}

	s.conn = conn
	err = s.writeBlockReadRequest()
	if err != nil {
		return err
	}

	s.reader = bufio.NewReader(s.conn)
	resp, err := s.readBlockReadResponse()
	if err != nil {
		return err
	}

	checksumInfo := resp.GetReadOpChecksumInfo().GetChecksum()
	checksumType := checksumInfo.GetType()
	if checksumType == hdfs.ChecksumTypeProto_CHECKSUM_CRC32 {
		s.checksumTab = crc32.IEEETable
	} else if checksumType == hdfs.ChecksumTypeProto_CHECKSUM_CRC32C {
		s.checksumTab = crc32.MakeTable(crc32.Castagnoli)
	} else {
		return fmt.Errorf("Unsupported checksum type: %d", checksumType)
	}

	s.chunkSize = checksumInfo.GetBytesPerChecksum()
	s.startNewPacket()

	// The read will start aligned to a chunk boundary, so we need to seek forward
	// to the requested offset.
	amountToDiscard := s.startOffset - s.packet.blockOffset
	if amountToDiscard > 0 {
		io.CopyN(ioutil.Discard, s, int64(amountToDiscard))
	}

	return nil
}

func (s *blockStream) Close() {
	s.conn.Close()
	s.closed = true
}

func (s *blockStream) Read(b []byte) (int, error) {
	if s.closed {
		return 0, io.ErrClosedPipe
	}

	// first, read any leftover data from buf
	if s.buf.Len() > 0 {
		n, _ := s.buf.Read(b)
		return n, nil
	}

	if s.packet.nextChunk >= s.packet.numChunks {
		if s.packet.last {
			return 0, io.EOF
		}

		s.startNewPacket()
	}

	// then, read until we fill up b or we reach the end of the packet
	readOffset := 0
	for s.packet.nextChunk < s.packet.numChunks {
		chOff := 4 * s.packet.nextChunk
		checksum := s.packet.checksumBytes[chOff : chOff+4]

		remaining := s.packet.length - s.packet.packetOffset
		chunkLength := int64(math.Min(float64(s.chunkSize), float64(remaining)))

		chunkReader := io.LimitReader(s.reader, int64(chunkLength))
		chunkBytes := b[readOffset:]
		bytesToRead := int(math.Min(float64(len(chunkBytes)), float64(chunkLength)))
		n, err := io.ReadAtLeast(chunkReader, chunkBytes, bytesToRead)

		readOffset += n
		s.packet.packetOffset += uint64(n)
		s.packet.nextChunk++

		if err != nil {
			s.Close()
			return readOffset, err
		}

		crc := crc32.Checksum(chunkBytes[:n], s.checksumTab)

		done := false
		if readOffset == len(b) {
			done = true

			if int64(n) < chunkLength {
				// save any leftovers
				s.buf.Reset()
				leftover, err := s.buf.ReadFrom(chunkReader)
				if err != nil {
					return readOffset, err
				}

				s.packet.packetOffset += uint64(leftover)

				// update the checksum with the leftovers
				crc = crc32.Update(crc, s.checksumTab, s.buf.Bytes())
			}
		}

		if crc != binary.BigEndian.Uint32(checksum) {
			return readOffset, errors.New("Invalid checksum from the datanode!")
		}

		if done {
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
func (s *blockStream) writeBlockReadRequest() error {
	header := []byte{0x00, dataTransferVersion, readBlockOp}

	needed := (s.block.GetB().GetNumBytes() - s.startOffset)
	op := newReadBlockOp(s.block, s.startOffset, needed)
	opBytes, err := makeDelimitedMsg(op)
	if err != nil {
		return err
	}

	req := append(header, opBytes...)
	_, err = s.conn.Write(req)
	if err != nil {
		return err
	}

	return nil
}

// The initial response from the datanode:
// +-----------------------------------------------------------+
// |  varint length + BlockOpResponseProto                     |
// +-----------------------------------------------------------+
func (s *blockStream) readBlockReadResponse() (*hdfs.BlockOpResponseProto, error) {
	respLength, err := binary.ReadUvarint(s.reader)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}

		return nil, err
	}

	respBytes := make([]byte, respLength)
	_, err = io.ReadFull(s.reader, respBytes)
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
func (s *blockStream) startNewPacket() error {
	header, err := s.readPacketHeader()
	if err != nil {
		return err
	}

	blockOffset := uint64(header.GetOffsetInBlock())
	dataLength := uint64(header.GetDataLen())
	numChunks := int(math.Ceil(float64(dataLength) / float64(s.chunkSize)))

	// TODO don't assume checksum size is 4
	s.packet = openPacket{
		numChunks:     numChunks,
		nextChunk:     0,
		checksumBytes: make([]byte, numChunks*4),
		blockOffset:   blockOffset,
		packetOffset:  0,
		length:        dataLength,
		last:          header.GetLastPacketInBlock(),
	}

	_, err = io.ReadFull(s.reader, s.packet.checksumBytes)
	if err != nil {
		return err
	}

	return nil
}

func (s *blockStream) readPacketHeader() (*hdfs.PacketHeaderProto, error) {
	var packetLength uint32
	err := binary.Read(s.reader, binary.BigEndian, &packetLength)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}

		return nil, err
	}

	var packetHeaderLength uint16
	err = binary.Read(s.reader, binary.BigEndian, &packetHeaderLength)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}

		return nil, err
	}

	packetHeaderBytes := make([]byte, packetHeaderLength)
	_, err = io.ReadFull(s.reader, packetHeaderBytes)
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
