#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/truncate
  $HDFS touch /_test_cmd/truncate/a
}

@test "truncate larger" {
  run $HDFS truncate 10 /_test_cmd/a
  assert_failure
}

@test "truncate nonexistent" {
  run $HDSF truncate 10 /_test_cmd/nonexistent
  assert_failure
}

@test "truncate" {
  run $HDFS put $ROOT_TEST_DIR/testdata/foo.txt /_test_cmd/truncate/1
  run $HDFS truncate 2 /_test_cmd/truncate/1
  assert_success
}

teardown() {
  $HDFS rm -r /_test_cmd/truncate
}
