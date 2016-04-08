package hdfs

import (
	"encoding/xml"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/colinmarc/hdfs/rpc"
)

type Property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type Result struct {
	Property []Property `xml:"property"`
}

type NameNode struct {
	Host string
	Port int
}

// Get Hadoop Properties
func GetHadoopProperties(path string) ([]Property, error) {
	result := Result{}
	f, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	xmlErr := xml.Unmarshal(f, &result)
	if xmlErr != nil {
		return nil, xmlErr
	}

	return result.Property, nil
}

// Get Namenode server name(s) from HDFS config
func GetNamenodesFromHDFSConfig(path string) []string {
	props, err := GetHadoopProperties(path)
	if err != nil {
		return []string{}
	}
	var nns []string
	for _, prop := range props {
		if strings.HasPrefix(prop.Name, "dfs.namenode.rpc-address") {
			nnUrl, _ := url.Parse(prop.Value)
			nns = append(nns, nnUrl.Host)
		}
	}
	return nns
}

// Get Namenode server name(s) from site config
func GetNamenodesFromSiteConfig(path string) []string {
	props, err := GetHadoopProperties(path)
	if err != nil {
		return []string{}
	}
	var nns []string
	for _, prop := range props {
		if strings.Contains(prop.Name, "fs.defaultFS") || strings.Contains(prop.Name, "fs.defaultFS") {
			nnUrl, _ := url.Parse(prop.Value)
			nns = append(nns, nnUrl.Host)
		}
	}
	return nns
}

// AutoConfigClient to create a client by trying to read the hadoop config
func GetAutoConfigClient() (*Client, error) {
	hadoopHome := os.Getenv("HADOOP_HOME")
	hadoopConfDir := os.Getenv("HADOOP_CONF_DIR")
	var tryPaths []string
	var confPaths []string
	if hadoopHome != "" {
		hdfsPath := path.Join(hadoopHome, "conf", "hdfs-site.xml")
		tryPaths = append(tryPaths, hdfsPath)
		corePath := path.Join(hadoopHome, "conf", "core-site.xml")
		confPaths = append(confPaths, corePath)
	}
	if hadoopConfDir != "" {
		confHdfsPath := path.Join(hadoopConfDir, "hdfs-site.xml")
		tryPaths = append(tryPaths, confHdfsPath)
		confCorePath := path.Join(hadoopConfDir, "core-site.xml")
		confPaths = append(confPaths, confCorePath)
	}
	var nameNodes []string
	for _, tryPath := range tryPaths {
		nameNodes = append(nameNodes, GetNamenodesFromHDFSConfig(tryPath)...)
	}
	for _, confPath := range confPaths {
		nameNodes = append(nameNodes, GetNamenodesFromSiteConfig(confPath)...)
	}

	address := nameNodes[0]
	username, err := Username()
	if err != nil {
		return nil, err
	}
	namenode, err := rpc.NewNamenodeConnection(address, username)
	if err != nil {
		return nil, err
	}

	return &Client{namenode: namenode}, nil
}
