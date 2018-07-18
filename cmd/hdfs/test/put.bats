#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/put/1
  $HDFS mkdir -p /_test_cmd/put/test
  $HDFS touch /_test_cmd/put/existing.txt
}

@test "put" {
  run $HDFS put $ROOT_TEST_DIR/testdata/foo.txt /_test_cmd/put/1
  assert_success

  run $HDFS cat /_test_cmd/put/1/foo.txt
  assert_output "bar"
}

@test "put long" {
  run $HDFS put $ROOT_TEST_DIR/testdata/mobydick.txt /_test_cmd/put/1
  assert_success

  run bash -c "$HDFS cat /_test_cmd/put/1/mobydick.txt > $BATS_TMPDIR/mobydick_test.txt"
  assert_success

  SHA=`shasum < $ROOT_TEST_DIR/testdata/mobydick.txt | awk '{ print $1 }'`
  assert_equal $SHA `shasum < $BATS_TMPDIR/mobydick_test.txt | awk '{ print $1 }'`
}

@test "put dir" {
  run $HDFS put $ROOT_TEST_DIR/testdata /_test_cmd/put/test2
  assert_success

  run $HDFS cat /_test_cmd/put/test2/foo.txt
  assert_output "bar"

  run bash -c "$HDFS cat /_test_cmd/put/test2/mobydick.txt > $BATS_TMPDIR/mobydick_test.txt"
  assert_success

  SHA=`shasum < $ROOT_TEST_DIR/testdata/mobydick.txt | awk '{ print $1 }'`
  assert_equal $SHA `shasum < $BATS_TMPDIR/mobydick_test.txt | awk '{ print $1 }'`
}


@test "put dir into dir" {
  run $HDFS put $ROOT_TEST_DIR/testdata /_test_cmd/put/test
  assert_success

  run $HDFS cat /_test_cmd/put/test/testdata/foo.txt
  assert_output "bar"

  run bash -c "$HDFS cat /_test_cmd/put/test/testdata/mobydick.txt > $BATS_TMPDIR/mobydick_test.txt"
  assert_success

  SHA=`shasum < $ROOT_TEST_DIR/testdata/mobydick.txt | awk '{ print $1 }'`
  assert_equal $SHA `shasum < $BATS_TMPDIR/mobydick_test.txt | awk '{ print $1 }'`
}

@test "put dir into file" {
  run $HDFS put $ROOT_TEST_DIR/testdata /_test_cmd/put/existing.txt
    assert_failure
    assert_output <<OUT
mkdir /_test_cmd/put/existing.txt: file already exists
OUT
}

@test "put stdin" {
  run bash -c "echo 'foo bar baz' | $HDFS put - /_test_cmd/put/stdin.txt"
  assert_success

  run $HDFS cat /_test_cmd/put/stdin.txt
  assert_output "foo bar baz"
}

@test "put stdin long" {
  run bash -c "cat $ROOT_TEST_DIR/testdata/mobydick.txt | $HDFS put - /_test_cmd/put/mobydick_stdin.txt"
  assert_success

  run bash -c "$HDFS cat /_test_cmd/put/mobydick_stdin.txt > $BATS_TMPDIR/mobydick_stdin_test.txt"
  assert_success

  SHA=`shasum < $ROOT_TEST_DIR/testdata/mobydick.txt | awk '{ print $1 }'`
  assert_equal $SHA `shasum < $BATS_TMPDIR/mobydick_stdin_test.txt | awk '{ print $1 }'`
}

@test "put stdin into file" {
  run bash -c "echo 'foo bar baz' | $HDFS put - /_test_cmd/put/existing.txt"
  assert_failure
  assert_output <<OUT
put /_test_cmd/put/existing.txt: file already exists
OUT
}

@test "put stdin into dir" {
  run bash -c "echo 'foo bar baz' | $HDFS put - /_test_cmd/put/1"
  assert_failure
  assert_output <<OUT
put /_test_cmd/put/1: file already exists
OUT
}

teardown() {
  $HDFS rm -r /_test_cmd/put
}
