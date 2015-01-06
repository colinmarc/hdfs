#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/du/dir1
  $HDFS mkdir -p /_test_cmd/du/dir2
  $HDFS mkdir -p /_test_cmd/du/dir3
  $HADOOP_FS -cp hdfs://$HADOOP_NAMENODE/_test/foo.txt hdfs://$HADOOP_NAMENODE/_test_cmd/du/dir1/foo1.txt
}

@test "du" {
  run $HDFS du /_test/foo.txt
  assert_success
  assert_output <<OUT
4       /_test/foo.txt
OUT
}

@test "du human readable" {
  run $HDFS du -h /_test/foo.txt
  assert_success
  assert_output <<OUT
4B      /_test/foo.txt
OUT
}


@test "du dir" {
  run $HDFS du /_test_cmd/du/dir1
  assert_success
  assert_output <<OUT
4       /_test_cmd/du/dir1/foo1.txt
4       /_test_cmd/du/dir1
OUT
}

@test "du summary" {
  run $HDFS du -s /_test_cmd/du/dir1
  assert_success
  assert_output <<OUT
4       /_test_cmd/du/dir1
OUT
}

@test "du nonexistent" {
  run $HDFS du /_test_cmd/nonexistent
  assert_failure
  assert_output <<OUT
stat /_test_cmd/nonexistent: file does not exist
OUT
}

teardown() {
  $HDFS rm -r /_test_cmd/du
}
