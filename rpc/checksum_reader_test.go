package rpc

import (
	"encoding/hex"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testChecksum = "b8d258c1ae6b31ce38b833f7e3bb5cb0"

func TestReadChecksum(t *testing.T) {
	if os.Getenv("KERBEROS") == "true" {
		// TODO: understand and fix perm issue ;)
		t.Skip("skipping due to permission issue with Kerberos.")
	}
	block := getBlocks(t, "/_test/mobydick.txt")[0]
	cr := NewChecksumReader(block)

	checksum, err := cr.ReadChecksum()
	require.NoError(t, err)
	assert.EqualValues(t, testChecksum, hex.EncodeToString(checksum))
}
