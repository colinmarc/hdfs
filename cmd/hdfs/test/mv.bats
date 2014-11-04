#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/mv/dir
  $HDFS touch /_test_cmd/mv/a
  $HDFS touch /_test_cmd/mv/b
  $HDFS touch /_test_cmd/mv/dir/c
}

@test "mv" {
  run $HDFS mv /_test_cmd/mv/a /_test_cmd/mv/moved
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mv/a
  assert_failure

  run $HDFS ls /_test_cmd/mv/moved
  assert_success
}

@test "mv nonexistent" {
  run $HDFS mv /_test_cmd/nonexistent /_test_cmd/nonexistent2
  assert_failure
  assert_output <<OUT
rename /_test_cmd/nonexistent: file does not exist
OUT
}

@test "mv into dir" {
  run $HDFS mv /_test_cmd/mv/a /_test_cmd/mv/dir
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mv/a
  assert_failure

  run $HDFS ls /_test_cmd/mv/dir/a
  assert_success
}

@test "mv into existing" {
  run $HDFS mv /_test_cmd/mv/a /_test_cmd/mv/b
  assert_failure
  assert_output <<OUT
rename /_test_cmd/mv/b: file already exists
OUT
}

@test "mv into existing with -f" {
  run $HDFS mv -f /_test_cmd/mv/a /_test_cmd/mv/b
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mv/a
  assert_failure

  run $HDFS ls /_test_cmd/mv/b
  assert_success
}

@test "mv into dir with -f" {
  run $HDFS mv -f /_test_cmd/mv/a /_test_cmd/mv/dir
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mv/a
  assert_failure

  run $HDFS ls /_test_cmd/mv/dir/a
  assert_success
}

@test "mv with -T" {
  run $HDFS mv -T /_test_cmd/mv/a /_test_cmd/mv/moved
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mv/a
  assert_failure

  run $HDFS ls /_test_cmd/mv/moved
  assert_success
}

@test "mv into existing dir with -T" {
  run $HDFS mv -T /_test_cmd/mv/a /_test_cmd/mv/dir
  assert_failure
  assert_output <<OUT
rename /_test_cmd/mv/dir: file already exists
OUT
}

@test "mv into existing dir with -fT" {
  run $HDFS mv -fT /_test_cmd/mv/a /_test_cmd/mv/dir
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mv/a
  assert_failure

  run $HDFS ls /_test_cmd/mv/dir
  assert_success
  assert_output "/_test_cmd/mv/dir"
}

teardown() {
  $HDFS rm -r /_test_cmd/mv
}
