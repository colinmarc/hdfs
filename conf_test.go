package hdfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfFallback(t *testing.T) {
	os.Setenv("HADOOP_HOME", "test") // This will resolve to test/conf.
	os.Setenv("HADOOP_CONF_DIR", "test/conf2")

	confNamenodes := []string{"namenode1:8020", "namenode2:8020"}
	conf2Namenodes := []string{"namenode3:8020"}
	conf3Namenodes := []string{"namenode4:8020"}

	conf := LoadHadoopConf("test/conf3")
	nns, err := conf.Namenodes()
	assert.Nil(t, err)
	assert.EqualValues(t, conf3Namenodes, nns, "loading via specified path (test/conf3)")

	conf = LoadHadoopConf("")
	nns, err = conf.Namenodes()
	assert.Nil(t, err)
	assert.EqualValues(t, conf2Namenodes, nns, "loading via HADOOP_CONF_DIR (test/conf2)")

	os.Unsetenv("HADOOP_CONF_DIR")

	conf = LoadHadoopConf("")
	nns, err = conf.Namenodes()
	assert.Nil(t, err)
	assert.EqualValues(t, confNamenodes, nns, "loading via HADOOP_HOME (test/conf)")

	os.Unsetenv("HADOOP_HOME")
}

func TestConfWithViewfs(t *testing.T) {
	var nns []string
	var err error
	var newurl string

	os.Setenv("HADOOP_HOME", "test") // This will resolve to test/conf.
	os.Setenv("HADOOP_CONF_DIR", "test/conf-viewfs")

	snn2Addrs := []string{"master03.sunshine.sogou:8020", "master04.sunshine.sogou:8020"}

	conf := LoadHadoopConf("")

	defNsid := conf.DefaultNSID()
	assert.EqualValues(t, "nsX", defNsid, "check defaultNSID in (test/conf-viewfs)")

	nns, err = conf.Namenodes()
	assert.Nil(t, err)
	assert.EqualValues(t, []string{"nsX"}, nns, "loading via specified path (test/conf-viewfs)")

	nns, err = conf.AddressesByNameServiceID("SunshineNameNode2")
	assert.Nil(t, err)
	assert.EqualValues(t, snn2Addrs, nns, "loading via specified path (test/conf-viewfs)")

	newurl, err = conf.ViewfsReparseFilename("/user/sub")
	assert.Nil(t, err)
	assert.EqualValues(t, "hdfs://SunshineNameNode2/user/sub", newurl, "loading via specified path (test/conf-viewfs)")

	newurl, err = conf.ViewfsReparseFilename("hdfs://nsX/user/sub")
	assert.Nil(t, err)
	assert.EqualValues(t, "hdfs://SunshineNameNode2/user/sub", newurl, "loading via specified path (test/conf-viewfs)")

	os.Unsetenv("HADOOP_CONF_DIR")
	os.Unsetenv("HADOOP_HOME")
}
