#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/test/dir1
  $HDFS mkdir -p /_test_cmd/test/dir2
  $HDFS mkdir -p /_test_cmd/test/dir3
  $HDFS put $ROOT_TEST_DIR/testdata/empty.txt /_test_cmd/test/empty.txt
  $HDFS put $ROOT_TEST_DIR/testdata/foo.txt /_test_cmd/test/foo.txt
  $HDFS put $ROOT_TEST_DIR/testdata/foo.txt /_test_cmd/test/dir1/foo1.txt
}

@test "exists for existent file" {
  run $HDFS test -e /_test_cmd/test/foo.txt
  assert_success
}

@test "exists for existent dir" {
  run $HDFS test -e /_test_cmd/test/dir1
  assert_success
}

@test "exists for non existent file" {
  run $HDFS test -e /_test_cmd/test/bar.txt
  assert_failure
}

@test "exists for non existent dir" {
  run $HDFS test -e /_test_cmd/test/dir9
  assert_failure
}

@test "isfile for existent file" {
  run $HDFS test -f /_test_cmd/test/foo.txt
  assert_success
}

@test "isfile for existent dir" {
  run $HDFS test -f /_test_cmd/test/dir1
  assert_failure
}

@test "isfile for non existent file" {
  run $HDFS test -f /_test_cmd/test/bar.txt
  assert_failure
}

@test "isfile for non existent dir" {
  run $HDFS test -f /_test_cmd/test/dir9
  assert_failure
}

@test "isdir for existent file" {
  run $HDFS test -d /_test_cmd/test/foo.txt
  assert_failure
}

@test "isdir for existent dir" {
  run $HDFS test -d /_test_cmd/test/dir1
  assert_success
}

@test "isdir for non existent file" {
  run $HDFS test -d /_test_cmd/test/bar.txt
  assert_failure
}

@test "isdir for non existent dir" {
  run $HDFS test -d /_test_cmd/test/dir9
  assert_failure
}

@test "isnonempty for existent empty file" {
  run $HDFS test -s /_test_cmd/test/empty.txt
  assert_failure
}

@test "isnonempty for existent non empty file" {
  run $HDFS test -s /_test_cmd/test/foo.txt
  assert_success
}

@test "isnonempty for existent dir" {
  run $HDFS test -s /_test_cmd/test/dir1
  assert_failure
}

@test "isnonempty for non existent file" {
  run $HDFS test -s /_test_cmd/test/bar.txt
  assert_failure
}

@test "isnonempty for non existent dir" {
  run $HDFS test -s /_test_cmd/test/dir9
  assert_failure
}

@test "isempty for existent empty file" {
  run $HDFS test -z /_test_cmd/test/empty.txt
  assert_success
}

@test "isempty for existent non empty file" {
  run $HDFS test -z /_test_cmd/test/foo.txt
  assert_failure
}

# TOOD: check this outcome is correct
@test "isempty for existent dir" {
  run $HDFS test -z /_test_cmd/test/dir1
  assert_success
}

@test "isempty for non existent file" {
  run $HDFS test -z /_test_cmd/test/bar.txt
  assert_failure
}

@test "isempty for non existent dir" {
  run $HDFS test -z /_test_cmd/test/dir9
  assert_failure
}

teardown() {
  $HDFS rm -r /_test_cmd/test
}

teardown() {
  $HDFS rm -r /_test_cmd/test
}
