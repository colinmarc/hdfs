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

@test "ls recursive on nested dir" {
  run $HDFS ls -R /_test_cmd/ls
  assert_success
  assert_output <<OUT
dir1
dir2
dir3

/_test_cmd/ls/dir1:
a
b
c

/_test_cmd/ls/dir2:
d

/_test_cmd/ls/dir3:
OUT
}

@test "ls recursive on leaf dir" {
  run $HDFS ls -R /_test_cmd/ls/dir1
  assert_success
  assert_output <<OUT
a
b
c
OUT
}

@test "ls recursive on file" {
  run $HDFS ls -R /_test_cmd/ls/dir1/c
  assert_success
  assert_output <<OUT
/_test_cmd/ls/dir1/c
OUT
}

@test "ls single files" {
  run $HDFS ls /_test_cmd/ls/dir1/a /_test_cmd/ls/dir1/b
  assert_success
  assert_output <<OUT
/_test_cmd/ls/dir1/a
/_test_cmd/ls/dir1/b
OUT
}

@test "ls nonexistent" {
  run $HDFS ls /_test_cmd/nonexistent
  assert_failure
  assert_output <<OUT
stat /_test_cmd/nonexistent: file does not exist
OUT
}

@test "ls recursive nonexistent" {
  run $HDFS ls -R /_test_cmd/nonexistent
  assert_failure
  assert_output <<OUT
stat /_test_cmd/nonexistent: file does not exist
OUT
}

teardown() {
  $HDFS rm -r /_test_cmd/ls
}
