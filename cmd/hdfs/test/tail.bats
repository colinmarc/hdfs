#!/usr/bin/env bats

load helper

@test "tail" {
  run $HDFS tail /_test/foo.txt
  assert_success
  assert_output "bar"
}

@test "tail long" {
  run $HDFS tail /_test/mobydick.txt
  assert_success
  assert_output "$(tail $ROOT_TEST_DIR/testdata/mobydick.txt)"
}

@test "tail bytes" {
  run $HDFS tail -c 10 /_test/mobydick.txt
  assert_success
  assert_output "$(tail -c 10 $ROOT_TEST_DIR/testdata/mobydick.txt)"
}

@test "tail nonexistent" {
  run $HDFS tail /_test_cmd/nonexistent
  assert_failure
  assert_output <<OUT
open /_test_cmd/nonexistent: file does not exist
OUT
}
