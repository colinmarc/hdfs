package rpc

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"errors"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"hash/crc32"
	"io"
	"math"
)

// blockReadStream implements io.Reader for reading a packet stream for a single
// block from a single datanode. It uses packetReadStream to read individual
// packets.
type blockReadStream struct {
	reader      io.Reader
	checksumTab *crc32.Table
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

func newBlockReadStream(reader io.Reader, chunkSize uint32, checksumTab *crc32.Table) *blockReadStream {
	return &blockReadStream{
		reader:      reader,
		chunkSize:   chunkSize,
		checksumTab: checksumTab,
	}
}

func (s *blockReadStream) Read(b []byte) (int, error) {
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
func (s *blockReadStream) startNewPacket() error {
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

func (s *blockReadStream) readPacketHeader() (*hdfs.PacketHeaderProto, error) {
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
