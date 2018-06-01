package rpc

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNamenodeConnection_resolveConnection(t *testing.T) {
	conn := getNamenode(t)
	conn.markFailure(io.EOF)

	assert.Error(t, conn.resolveConnection())
	conn.host.lastErrorAt = time.Now().Add(-backoffDuration)
	assert.NoError(t, conn.resolveConnection())
	cachedNamenode = nil
}
