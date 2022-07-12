package hdfs

import (
	"bytes"
	"crypto/cipher"
	"testing"

	"github.com/stretchr/testify/assert"
)

// aesCtrRead perform AES-CTR XOR operation on given byte string.
// Once encryption and decryption are exactly the same operation for CTR mode,
// this function can be used to perform both.
func aesCtrStep(offset int64, enc *transparentEncryptionInfo, b []byte) ([]byte, error) {
	stream, err := aesCreateCTRStream(offset, enc)
	if err != nil {
		return nil, err
	}

	r := make([]byte, len(b))
	_, err = cipher.StreamReader{S: stream, R: bytes.NewReader(b)}.Read(r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func TestAesIV(t *testing.T) {
	originalText := []byte("some random plain text, nice to have it quite long")
	key := []byte("0123456789abcdef")

	// Choose iv to hit counter overflow.
	iv := []byte("\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xf5")
	enc := &transparentEncryptionInfo{iv: iv, key: key}

	// Ensure that we can decrypt text after encryption.
	// In CTR mode, implementation for `encrypt` and `decrypt` actually the same
	// since we just XOR on input.
	encryptedText, err := aesCtrStep(0, enc, originalText)
	assert.Equal(t, err, nil)
	decryptedText, err := aesCtrStep(0, enc, encryptedText)
	assert.Equal(t, err, nil)
	assert.Equal(t, originalText, decryptedText)

	// CTR mode allow us to encrypt/decrypt string by chunks
	// (using correct offset from start of string).
	// Ensure that result equal to one, produced in one step.
	encryptedByChunks := make([]byte, 0)
	var pos int64 = 0
	for _, x := range []int{5, 7, 6, 4, 28} {
		tmp, err := aesCtrStep(pos, enc, originalText[pos:pos+int64(x)])
		assert.Equal(t, err, nil)
		encryptedByChunks = append(encryptedByChunks, tmp...)
		pos += int64(x)
	}
	assert.Equal(t, encryptedByChunks, encryptedText)

	// Decrypt string by chunks.
	// Ensure that result equal to one, produced in one step.
	decryptedByChunks := make([]byte, 0)
	pos = 0
	for _, x := range []int{5, 7, 6, 4, 28} {
		tmp, err := aesCtrStep(pos, enc, encryptedText[pos:pos+int64(x)])
		assert.Equal(t, err, nil)
		decryptedByChunks = append(decryptedByChunks, tmp...)
		pos += int64(x)
	}
	assert.Equal(t, decryptedByChunks, decryptedText)
}
