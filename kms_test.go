package hdfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKmsParseProviderUri(t *testing.T) {
	assert.Equal(t, nil, nil)

	urls, err := kmsParseProviderUri("")
	assert.Error(t, err)

	urls, err = kmsParseProviderUri("http")
	assert.Error(t, err)

	urls, err = kmsParseProviderUri("kms://https@localhost:9600/kms")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(urls))
	assert.Equal(t, "https://localhost:9600/kms", urls[0])

	urls, err = kmsParseProviderUri("kms://http@kms01.example.com:9600;kms02.example.com")
	assert.Error(t, err)

	urls, err = kmsParseProviderUri("kms://http@kms01.example.com/kms;kms02.example.com")
	assert.Error(t, err)

	urls, err = kmsParseProviderUri("kms://http@kms01.example.com;kms02.example.com:9600/kms")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(urls))
	assert.Equal(t, "http://kms01.example.com:9600/kms", urls[0])
	assert.Equal(t, "http://kms02.example.com:9600/kms", urls[1])

	urls, err = kmsParseProviderUri("kms://http@kms01.example.com;kms02.example.com/kms")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(urls))
	assert.Equal(t, "http://kms01.example.com:9600/kms", urls[0])
	assert.Equal(t, "http://kms02.example.com:9600/kms", urls[1])

	urls, err = kmsParseProviderUri("kms://http@kms01.example.com;kms02.example.com:9600")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(urls))
	assert.Equal(t, "http://kms01.example.com:9600", urls[0])
	assert.Equal(t, "http://kms02.example.com:9600", urls[1])

	urls, err = kmsParseProviderUri("kms://http@kms01.example.com;kms02.example.com;kms03.example.com")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(urls))
	assert.Equal(t, "http://kms01.example.com:9600", urls[0])
	assert.Equal(t, "http://kms02.example.com:9600", urls[1])
	assert.Equal(t, "http://kms03.example.com:9600", urls[2])
}
