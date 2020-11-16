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

@test "ls -d" {
  run $HDFS ls -d /_test_cmd/ls/dir1
  assert_success
  assert_output <<OUT
/_test_cmd/ls/dir1
OUT
}

@test "ls -d Wildcard" {
  run $HDFS ls -d /_test_cmd/ls/dir*
  assert_success
  assert_output <<OUT
/_test_cmd/ls/dir1
/_test_cmd/ls/dir2
/_test_cmd/ls/dir3
OUT
}

@test "ls -ld" {
  run $HDFS ls -ld /_test_cmd/ls
  assert_success
  regex="^drwxr-xr-x [a-z]+  hadoop  0 [a-zA-Z]+ [0-9]+ [0-9]{2}:[0-9]{2} /_test_cmd/ls$"
  [[ $output =~ $regex ]]
}

@test "ls -ld Wildcard" {
  run $HDFS ls -ld /_test_cmd/ls/dir*
  assert_success
  regex="^(drwxr-xr-x [a-z]+  hadoop  0 [a-zA-Z]+ [0-9]+ [0-9]{2}:[0-9]{2} /_test_cmd/ls/dir[1-3].{0,1}){3}$"
  echo $output
  [[ $output =~ $regex ]]
}

@test "ls -R root" {
  run $HDFS ls -R /
  assert_success
}

@test "ls -R dir" {
  run $HDFS ls -R /_test_cmd/ls/
  echo $output
  assert_output <<OUT
/_test_cmd/ls:
total 3
dir1
dir2
dir3

/_test_cmd/ls/dir1:
total 3
a
b
c

/_test_cmd/ls/dir2:
total 1
d

/_test_cmd/ls/dir3:
total 0
OUT
}

@test "ls -R subdir" {
  run $HDFS ls -R /_test_cmd/ls/dir1
  echo $output
  assert_output <<OUT
/_test_cmd/ls/dir1:
total 3
a
b
c
OUT
}

teardown() {
  $HDFS rm -r /_test_cmd/ls
}
