package transfer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPacketSize(t *testing.T) {
	bws := &blockWriteStream{}
	bws.buf.Write(make([]byte, outboundPacketSize*3))
	packet := bws.makePacket()

	assert.EqualValues(t, outboundPacketSize, len(packet.data))
}

func TestPacketSizeUndersize(t *testing.T) {
	bws := &blockWriteStream{}
	bws.buf.Write(make([]byte, outboundPacketSize-5))
	packet := bws.makePacket()

	assert.EqualValues(t, outboundPacketSize-5, len(packet.data))
}

func TestPacketSizeAlignment(t *testing.T) {
	bws := &blockWriteStream{}
	bws.buf.Write(make([]byte, outboundPacketSize*3))

	bws.offset = 5
	packet := bws.makePacket()

	assert.EqualValues(t, outboundChunkSize-5, len(packet.data))
}
