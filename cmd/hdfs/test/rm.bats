#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/rm/dir
  $HDFS touch /_test_cmd/rm/a
  $HDFS touch /_test_cmd/rm/b
  $HDFS touch /_test_cmd/rm/dir/c
}

@test "rm" {
  run $HDFS rm /_test_cmd/rm/a
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/rm/a
  assert_failure
}

@test "rm dir" {
  run $HDFS rm -r /_test_cmd/rm/dir
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/rm/dir
  assert_failure
}

@test "rm dir without -r" {
  run $HDFS rm /_test_cmd/rm/dir
  assert_failure
  assert_output "remove /_test_cmd/rm/dir: file is a directory"
}

@test "rm dir without -r, but with -f" {
  run $HDFS rm -f /_test_cmd/rm/dir
  assert_failure
  assert_output "remove /_test_cmd/rm/dir: file is a directory"
}

@test "rm nonexistent" {
  run $HDFS rm /_test_cmd/nonexistent /_test_cmd/nonexistent2
  assert_failure
  assert_output <<OUT
remove /_test_cmd/nonexistent: file does not exist
remove /_test_cmd/nonexistent2: file does not exist
OUT
}

@test "rm nonexistent with -f" {
  run $HDFS rm -f /_test_cmd/nonexistent /_test_cmd/nonexistent2
  assert_success
  assert_output ""
}

teardown() {
  $HDFS rm -r /_test_cmd/rm
}
