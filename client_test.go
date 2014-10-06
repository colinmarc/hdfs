package hdfs

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"log"
)

func getClient(t *testing.T) *Client {
	nn := os.Getenv("HADOOP_NAMENODE")
	if nn == "" {
		t.Fatal("HADOOP_NAMENODE not set")
	}

	client, err := New(nn)
	if err != nil {
		t.Fatal(err)
	}

	return client
}

func TestStat(t *testing.T) {
	client := getClient(t)

	resp, err := client.Stat("/foo")
	assert.Nil(t, err)

	assert.Equal(t, resp.Name(), "/foo")
	log.Println(resp.ModTime())
	log.Println(resp.Size())
}
