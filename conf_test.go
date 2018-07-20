package hdfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfFallback(t *testing.T) {
	oldHome := os.Getenv("HADOOP_HOME")
	oldConfDir := os.Getenv("HADOOP_CONF_DIR")
	os.Setenv("HADOOP_HOME", "testdata") // This will resolve to testdata/conf.
	os.Setenv("HADOOP_CONF_DIR", "testdata/conf2")

	confNamenodes := []string{"namenode1:8020", "namenode2:8020"}
	conf2Namenodes := []string{"namenode3:8020"}
	conf3Namenodes := []string{"namenode4:8020"}

	conf := LoadHadoopConf("testdata/conf3")
	nns, err := conf.Namenodes()
	assert.Nil(t, err)
	assert.EqualValues(t, conf3Namenodes, nns, "loading via specified path (testdata/conf3)")

	conf = LoadHadoopConf("")
	nns, err = conf.Namenodes()
	assert.Nil(t, err)
	assert.EqualValues(t, conf2Namenodes, nns, "loading via HADOOP_CONF_DIR (testdata/conf2)")

	os.Unsetenv("HADOOP_CONF_DIR")

	conf = LoadHadoopConf("")
	nns, err = conf.Namenodes()
	assert.Nil(t, err)
	assert.EqualValues(t, confNamenodes, nns, "loading via HADOOP_HOME (testdata/conf)")

	os.Setenv("HADOOP_HOME", oldHome)
	os.Setenv("HADOOP_CONF_DIR", oldConfDir)
}
