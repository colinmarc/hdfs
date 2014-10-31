#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/ls/dir1
  $HDFS mkdir -p /_test_cmd/ls/dir2
  $HDFS mkdir -p /_test_cmd/ls/dir3
  $HDFS touch /_test_cmd/ls/dir1/a
  $HDFS touch /_test_cmd/ls/dir1/b
  $HDFS touch /_test_cmd/ls/dir1/c
  $HDFS touch /_test_cmd/ls/dir2/d
}

@test "ls" {
  run $HDFS ls /_test_cmd/ls/dir1
  assert_success
  assert_output <<OUT
a
b
c
OUT
}

@test "ls nonexistent" {
  run $HDFS ls /_test_cmd/nonexistent
  assert_failure
  assert_output <<OUT
stat /_test_cmd/nonexistent: file does not exist
OUT
}

@test "ls with glob" {
  run $HDFS ls /_test_cmd/ls/dir*
  assert_success
  assert_output <<OUT
/_test_cmd/ls/dir1/:
a
b
c

/_test_cmd/ls/dir2/:
d

/_test_cmd/ls/dir3/:
OUT
}

@test "ls with two globs, one of which is qualified" {
  run $HDFS ls /_test_cmd/ls/dir*/*
  assert_success
  assert_output <<OUT
a
b
c
d
OUT
}

@test "ls with two globs" {
  run $HDFS ls /_test_cmd/ls/*/*
  assert_success
  assert_output <<OUT
a
b
c
d
OUT
}

teardown() {
  $HDFS rm -r /_test_cmd/ls
}
