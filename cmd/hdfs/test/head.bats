#!/usr/bin/env bats

load helper

@test "head" {
  run $HDFS head /_test/foo.txt
  assert_success
  assert_output "bar"
}

@test "head long" {
  run $HDFS head /_test/mobydick.txt
  assert_success
  assert_output "$(head $ROOT_TEST_DIR/testdata/mobydick.txt)"
}

@test "head bytes" {
  run $HDFS head -c 10 /_test/mobydick.txt
  assert_success
  assert_output "$(head -c 10 $ROOT_TEST_DIR/testdata/mobydick.txt)"
}

@test "head nonexistent" {
  run $HDFS head /_test_cmd/nonexistent
  assert_failure
  assert_output <<OUT
open /_test_cmd/nonexistent: file does not exist
OUT
}
