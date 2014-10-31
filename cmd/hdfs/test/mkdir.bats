#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/mkdir/existing
}

@test "mkdir" {
  run $HDFS mkdir /_test_cmd/mkdir/a /_test_cmd/mkdir/b
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mkdir/a
  assert_success

  run $HDFS ls /_test_cmd/mkdir/b
  assert_success
}

@test "mkdir nonexistent" {
  run $HDFS mkdir /_test_cmd/nonexistent/a
  assert_failure
  assert_output <<OUT
mkdir /_test_cmd/nonexistent/a: file does not exist
OUT
}

@test "mkdir nonexistent with -p" {
  run $HDFS mkdir -p /_test_cmd/mkdir/nonexistent/a
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mkdir/nonexistent/a
  assert_success
}

teardown() {
  $HDFS rm -r /_test_cmd/mkdir
}
