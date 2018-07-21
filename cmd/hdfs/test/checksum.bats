#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/checksum/dir
}

@test "checksum" {
  FOO_CHECKSUM=$($HADOOP_FS -checksum hdfs://$HADOOP_NAMENODE/_test/foo.txt | tail -1 | awk '{ print substr($3, 25, 32) }')

  run $HDFS checksum /_test/foo.txt
  assert_success
  assert_output <<OUT
$FOO_CHECKSUM /_test/foo.txt
OUT
}

@test "checksum nonexistent" {
  run $HDFS cat /_test_cmd/nonexistent
  assert_failure
  assert_output <<OUT
open /_test_cmd/nonexistent: file does not exist
OUT
}

teardown() {
  $HDFS rm -r /_test_cmd/checksum
}
