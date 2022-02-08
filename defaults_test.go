package hdfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerDefaults(t *testing.T) {
	client := getClient(t)

	sd, err := client.ServerDefaults()
	require.NoError(t, err)
	assert.NotZero(t, sd.BlockSize)
}
