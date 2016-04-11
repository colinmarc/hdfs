package hdfs

import (
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfNamenodeHadoopHome(t *testing.T) {
	pwd, _ := os.Getwd()
	os.Setenv("HADOOP_HOME", strings.Join([]string{pwd, "test"}, "/"))
	hdConf := LoadHadoopConf("")
	nns, err := hdConf.Namenodes()
	assert.Nil(t, err)
	sort.Strings(nns)
	expectedNns := []string{"hadoop-namenode-01:8020", "hadoop-namenode-02:8020", "testnode:9000"}
	assert.EqualValues(t, expectedNns, nns)
	os.Setenv("HADOOP_HOME", "")
}

func TestConfNamenodeHadoopConfDir(t *testing.T) {
	pwd, _ := os.Getwd()
	os.Setenv("HADOOP_CONF_DIR", strings.Join([]string{pwd, "test"}, "/"))
	hdConf := LoadHadoopConf("")
	nns, err := hdConf.Namenodes()
	expectedNns := []string{"testnode:9000"}
	assert.Nil(t, err)
	assert.EqualValues(t, nns, expectedNns)
	os.Setenv("HADOOP_CONF_DIR", "")
}

func TestDedupeNamenodes(t *testing.T) {
	pwd, _ := os.Getwd()
	os.Setenv("HADOOP_HOME", strings.Join([]string{pwd, "test"}, "/"))
	os.Setenv("HADOOP_CONF_DIR", strings.Join([]string{pwd, "test"}, "/"))
	hdConf := LoadHadoopConf("")
	nns, err := hdConf.Namenodes()
	assert.Nil(t, err)
	sort.Strings(nns)
	expectedNns := []string{"hadoop-namenode-01:8020", "hadoop-namenode-02:8020", "testnode:9000"}
	assert.EqualValues(t, expectedNns, nns)

	os.Setenv("HADOOP_HOME", "")
	os.Setenv("HADOOP_CONF_DIR", "")
}
