#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/count/dir1
  $HDFS mkdir -p /_test_cmd/count/dir2
  $HDFS mkdir -p /_test_cmd/count/dir3
  $HDFS put $ROOT_TEST_DIR/testdata/foo.txt /_test_cmd/count/foo.txt
  $HDFS put $ROOT_TEST_DIR/testdata/foo.txt /_test_cmd/count/dir1/foo1.txt
}

@test "count" {
  run $HDFS count /_test_cmd/count/foo.txt
  assert_success
  assert_output <<OUT
                 0                        1                        4     /_test_cmd/count/foo.txt 
OUT
}

@test "count human readable" {
  run $HDFS count -h /_test_cmd/count/foo.txt
  assert_success
  assert_output <<OUT
                 0                        1                       4B     /_test_cmd/count/foo.txt 
OUT
}

@test "count dir" {
  run $HDFS count /_test_cmd/count
  assert_success
  assert_output <<OUT
                 4                        2                        8             /_test_cmd/count 
OUT
}

@test "count wildcard" {
  run $HDFS count /_test_cmd/count/dir*
  assert_success
  assert_output <<OUT
                 1                        1                        4        /_test_cmd/count/dir1 
                 1                        0                        0        /_test_cmd/count/dir2 
                 1                        0                        0        /_test_cmd/count/dir3 
OUT
}

@test "count nonexistent" {
  run $HDFS count /_test_cmd/nonexistent
  assert_failure
  assert_output <<OUT
stat /_test_cmd/nonexistent: file does not exist
OUT
}

teardown() {
  $HDFS rm -r /_test_cmd/count
}
