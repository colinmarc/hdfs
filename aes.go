package hdfs

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
)

// calculateIV `shifts` IV to given offset
// based on calculateIV from AesCtrCryptoCodec.java
func calculateIV(offset int64, initIV []byte) ([]byte, error) {
	if len(initIV) != aes.BlockSize {
		return nil, fmt.Errorf("calculateIV: invalid iv size: %v", len(initIV))
	}

	counter := offset / aes.BlockSize
	iv := make([]byte, aes.BlockSize)

	high := binary.BigEndian.Uint64(initIV[:8])
	low := binary.BigEndian.Uint64(initIV[8:])
	origLow := low

	low += uint64(counter)
	if low < origLow { // wrap
		high += 1
	}

	binary.BigEndian.PutUint64(iv, high)
	binary.BigEndian.PutUint64(iv[8:], low)

	return iv, nil
}

// aesCreateCTRStream create stream to encrypt/decrypt data from specific offset
func aesCreateCTRStream(offset int64, enc *transparentEncryptionInfo) (cipher.Stream, error) {
	iv, err := calculateIV(offset, enc.iv)
	if err != nil {
		return nil, err
	}

	if enc.cipher == nil {
		cipher, err := aes.NewCipher(enc.key)
		if err != nil {
			return nil, err
		}
		enc.cipher = cipher
	}

	stream := cipher.NewCTR(enc.cipher, iv)

	padding := offset % aes.BlockSize
	if padding > 0 {
		tmp := make([]byte, padding)
		stream.XORKeyStream(tmp, tmp)
	}
	return stream, nil
}
