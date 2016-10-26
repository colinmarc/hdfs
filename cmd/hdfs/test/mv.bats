#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/mv/dir1
  $HDFS mkdir -p /_test_cmd/mv/dir2
  $HDFS touch /_test_cmd/mv/a
  $HDFS touch /_test_cmd/mv/b
  $HDFS touch /_test_cmd/mv/dir1/c
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
  run $HDFS mv /_test_cmd/mv/a /_test_cmd/mv/dir1
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mv/a
  assert_failure

  run $HDFS ls /_test_cmd/mv/dir1/a
  assert_success
}

@test "mv into existing" {
  run $HDFS mv /_test_cmd/mv/a /_test_cmd/mv/b
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mv/a
  assert_failure

  run $HDFS ls /_test_cmd/mv/b
  assert_success
}

@test "mv into existing with -n" {
  run $HDFS mv -n /_test_cmd/mv/a /_test_cmd/mv/b
  assert_failure
  assert_output <<OUT
rename /_test_cmd/mv/b: file already exists
OUT
}

@test "mv into dir with -n" {
  run $HDFS mv -n /_test_cmd/mv/a /_test_cmd/mv/dir1
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mv/a
  assert_failure

  run $HDFS ls /_test_cmd/mv/dir1/a
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
  run $HDFS mv -T /_test_cmd/mv/a /_test_cmd/mv/dir1
  assert_failure
  assert_output <<OUT
Can't replace directory with non-directory.
OUT
}

@test "mv into existing dir with -nT" {
  run $HDFS mv -nT /_test_cmd/mv/a /_test_cmd/mv/dir1
  assert_failure
  assert_output <<OUT
Can't replace directory with non-directory.
OUT
}

@test "mv dir into existing dir" {
  run $HDFS mv /_test_cmd/mv/dir1 /_test_cmd/mv/dir2
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mv/dir1
  assert_failure

  run $HDFS ls /_test_cmd/mv/dir2
  assert_success
}

@test "mv dir into existing dir with -T" {
  run $HDFS mv -T /_test_cmd/mv/dir1 /_test_cmd/mv/dir2
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/mv/dir1
  assert_failure

  run $HDFS ls /_test_cmd/mv/dir2
  assert_success
}

@test "mv dir into existing dir with -nT" {
  run $HDFS mv -nT /_test_cmd/mv/dir1 /_test_cmd/mv/dir2
  assert_failure
  assert_output <<OUT
rename /_test_cmd/mv/dir2: file already exists
OUT
}

teardown() {
  $HDFS rm -r /_test_cmd/mv
}
