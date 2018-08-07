#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/glob/dir1/dir/
  $HDFS mkdir -p /_test_cmd/glob/dir2/dir/
  $HDFS mkdir -p /_test_cmd/glob/dir3/
  $HDFS touch /_test_cmd/glob/dir1/foo
  $HDFS touch /_test_cmd/glob/dir1/dir/a
  $HDFS touch /_test_cmd/glob/dir1/dir/b
  $HDFS touch /_test_cmd/glob/dir1/dir/c
  $HDFS touch /_test_cmd/glob/dir2/dir/d
}

@test "ls with glob" {
  run $HDFS ls /_test_cmd/glob/dir*/dir
  assert_success
  assert_output <<OUT
/_test_cmd/glob/dir1/dir/:
a
b
c

/_test_cmd/glob/dir2/dir/:
d
OUT
}

@test "ls with two globs" {
  run $HDFS ls /_test_cmd/glob/*/*
  assert_success
  assert_output <<OUT
/_test_cmd/glob/dir1/foo

/_test_cmd/glob/dir1/dir/:
a
b
c

/_test_cmd/glob/dir2/dir/:
d
OUT
}

@test "ls with two globs, one of which is qualified" {
  run $HDFS ls /_test_cmd/glob/dir*/*
  assert_success
  assert_output <<OUT
/_test_cmd/glob/dir1/foo

/_test_cmd/glob/dir1/dir/:
a
b
c

/_test_cmd/glob/dir2/dir/:
d
OUT
}

@test "ls with two globs, two of which are qualified" {
  run $HDFS ls /_test_cmd/glob/dir*/dir*
  assert_success
  assert_output <<OUT
/_test_cmd/glob/dir1/dir/:
a
b
c

/_test_cmd/glob/dir2/dir/:
d
OUT
}

@test "ls with three globs" {
  run $HDFS ls /_test_cmd/glob/*/*/*
  assert_success
  assert_output <<OUT
/_test_cmd/glob/dir1/dir/a
/_test_cmd/glob/dir1/dir/b
/_test_cmd/glob/dir1/dir/c
/_test_cmd/glob/dir2/dir/d
OUT
}

# qualify the files portion unless there's one path and it's a directory

@test "ls with three globs, one of which is qualified" {
  run $HDFS ls /_test_cmd/glob/dir*/*/*
  assert_success
  assert_output <<OUT
/_test_cmd/glob/dir1/dir/a
/_test_cmd/glob/dir1/dir/b
/_test_cmd/glob/dir1/dir/c
/_test_cmd/glob/dir2/dir/d
OUT
}

@test "ls nonexistent blob" {
  run $HDFS ls /_test_cmd/nonexistent*
  assert_failure
  assert_output <<OUT
stat /_test_cmd/nonexistent*: file does not exist
OUT
}

# teardown() {
#   $HDFS rm -r /_test_cmd/glob
# }
