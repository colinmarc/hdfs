package rpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const replacementSPNHost = "nn1.foo.com"

func TestReplaceSPNHostWildcard(t *testing.T) {
	tests := []struct {
		SPN      string
		Expected string
	}{
		{"nn/example.com", "nn/example.com"},
		{"nn/_HOST", "nn/nn1.foo.com"},
		{"nn/_HOST@EXAMPLE.COM", "nn/nn1.foo.com@EXAMPLE.COM"},
		{"nn/_HOST/EXAMPLE.COM", "nn/nn1.foo.com/EXAMPLE.COM"},
		{"nn/_HOSTFOO.COM", "nn/_HOSTFOO.COM"},
		{"_HOST", "_HOST"},
		{"/_HOST@EXAMPLE.COM", "/_HOST@EXAMPLE.COM"},
		{"_HOST/nn2.foo.com", "_HOST/nn2.foo.com"},
		{"nn/nn2.foo.com/_HOST", "nn/nn2.foo.com/_HOST"},
	}

	for _, test := range tests {
		t.Run("SPN="+test.SPN, func(t *testing.T) {
			assert.Equal(t, test.Expected, replaceSPNHostWildcard(test.SPN, replacementSPNHost))
		})
	}
}
