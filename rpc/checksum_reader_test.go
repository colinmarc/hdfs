package rpc

import (
	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

const testChecksum = "b8d258c1ae6b31ce38b833f7e3bb5cb0"

func TestReadChecksum(t *testing.T) {
	block := getBlocks(t, "/_test/mobydick.txt")[0]
	cr := NewChecksumReader(block)

	checksum, err := cr.ReadChecksum()
	require.Nil(t, err)
	assert.Equal(t, testChecksum, hex.EncodeToString(checksum))
}
