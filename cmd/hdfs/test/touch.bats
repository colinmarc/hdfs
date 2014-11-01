#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/touch
  $HDFS touch /_test_cmd/touch/existing
}

@test "touch" {
  run $HDFS touch /_test_cmd/touch/a /_test_cmd/touch/b
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/touch/a
  assert_success

  run $HDFS ls /_test_cmd/touch/b
  assert_success
}

@test "touch nonexistent" {
  run $HDFS touch /_test_cmd/nonexistent/a
  assert_failure
  assert_output <<OUT
create /_test_cmd/nonexistent/a: file does not exist
OUT
}

teardown() {
  $HDFS rm -r /_test_cmd/touch
}
