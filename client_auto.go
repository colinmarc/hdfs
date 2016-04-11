package hdfs

import (
	"encoding/xml"
	"errors"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"
)

type Property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type Result struct {
	Property []Property `xml:"property"`
}

type HadoopConf map[string]string

var ErrUnresolvedNamenode = errors.New("Couldn't find a namenode address in any config.")

// Get Hadoop Properties - try to open a conf file, marshal the results
// into a Result object and return the Properties of that object.
func LoadHadoopConfig(path string) (HadoopConf, error) {
	result := Result{}
	f, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	xmlErr := xml.Unmarshal(f, &result)
	if xmlErr != nil {
		return nil, xmlErr
	}

	hadoopConf := make(HadoopConf)
	for _, prop := range result.Property {
		hadoopConf[prop.Name] = prop.Value
	}
	return hadoopConf, nil
}

func (conf HadoopConf) Namenodes() []string {
	var nns []string
	for key, value := range conf {
		if strings.Contains(key, "fs.defaultFS") {
			nnUrl, _ := url.Parse(value)
			nns = append(nns, nnUrl.Host)
		}
		if strings.HasPrefix(key, "dfs.namenode.rpc-address") {
			nns = append(nns, value)
		}
	}
	return nns
}

// Return first namenode address we find in the hadoop config files
// else we try and return HADOOP_NAMENODE env var value else err
func GetNamenodeFromConfig() (string, error) {
	hadoopHome := os.Getenv("HADOOP_HOME")
	hadoopConfDir := os.Getenv("HADOOP_CONF_DIR")
	var tryPaths []string
	if hadoopHome != "" {
		hdfsPath := path.Join(hadoopHome, "conf", "hdfs-site.xml")
		corePath := path.Join(hadoopHome, "conf", "core-site.xml")
		tryPaths = append(tryPaths, []string{hdfsPath, corePath}...)
	}
	if hadoopConfDir != "" {
		confHdfsPath := path.Join(hadoopConfDir, "hdfs-site.xml")
		confCorePath := path.Join(hadoopConfDir, "core-site.xml")
		tryPaths = append(tryPaths, []string{confHdfsPath, confCorePath}...)
	}
	var nameNodes []string
	for _, tryPath := range tryPaths {
		hadoopConf, err := LoadHadoopConfig(tryPath)
		if err == nil {
			nameNodes = append(nameNodes, hadoopConf.Namenodes()...)
		}
	}

	var address string
	if len(nameNodes) > 0 {
		address = nameNodes[0]
	} else if os.Getenv("HADOOP_NAMENODE") != "" {
		address = os.Getenv("HADOOP_NAMENODE")
	} else {
		return "", ErrUnresolvedNamenode
	}

	return address, nil
}
