#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_cloud/checksum/dir
  export HADOOP_CONF_DIR=$ROOT_TEST_DIR/test/conf-viewfs
}

@test "[viewfs] checksum" {
  FOO_CHECKSUM=$($HADOOP_FS -checksum hdfs://$HADOOP_NAMENODE/_cloud/foo.txt | awk '/MD5-of-0MD5/{ print substr($3, 25, 32) }')

  run $HDFS checksum viewfs://nsX/cloud/foo.txt
  assert_success
  assert_output <<OUT
$FOO_CHECKSUM /cloud/foo.txt
OUT
}

@test "[viewfs] checksum nonexistent" {
  run $HDFS cat /cloud/nonexistent
  assert_failure
  assert_output <<OUT
open /cloud/nonexistent: file does not exist
OUT
}

@test "[viewfs] cat nonexistent" {
  run $HDFS cat /cloud_nonexistent/foo.txt
  assert_failure
  assert_output <<OUT
open /cloud_nonexistent/foo.txt: file does not exist
OUT
}

teardown() {
  $HDFS rm -r /_cloud/checksum
}
