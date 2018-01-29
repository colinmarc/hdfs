#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/put/1
  $HDFS mkdir -p /_test_cmd/put/test
  $HDFS touch /_test_cmd/put/existing.txt
}

@test "put" {
  run $HDFS put $ROOT_TEST_DIR/test/foo.txt /_test_cmd/put/1
  assert_success

  run $HDFS cat /_test_cmd/put/1/foo.txt
  assert_output "bar"
}

@test "put long" {
  run $HDFS put $ROOT_TEST_DIR/test/mobydick.txt /_test_cmd/put/1
  assert_success

  run bash -c "$HDFS cat /_test_cmd/put/1/mobydick.txt > $BATS_TMPDIR/mobydick_test.txt"
  assert_success

  SHA=`shasum < $ROOT_TEST_DIR/test/mobydick.txt | awk '{ print $1 }'`
  assert_equal $SHA `shasum < $BATS_TMPDIR/mobydick_test.txt | awk '{ print $1 }'`
}

@test "put dir" {
  run $HDFS put $ROOT_TEST_DIR/test /_test_cmd/put/test2
  assert_success

  run $HDFS cat /_test_cmd/put/test2/foo.txt
  assert_output "bar"

  run bash -c "$HDFS cat /_test_cmd/put/test2/mobydick.txt > $BATS_TMPDIR/mobydick_test.txt"
  assert_success

  SHA=`shasum < $ROOT_TEST_DIR/test/mobydick.txt | awk '{ print $1 }'`
  assert_equal $SHA `shasum < $BATS_TMPDIR/mobydick_test.txt | awk '{ print $1 }'`
}


@test "put dir into dir" {
  run $HDFS put $ROOT_TEST_DIR/test /_test_cmd/put/test
  assert_success

  run $HDFS cat /_test_cmd/put/test/test/foo.txt
  assert_output "bar"

  run bash -c "$HDFS cat /_test_cmd/put/test/test/mobydick.txt > $BATS_TMPDIR/mobydick_test.txt"
  assert_success

  SHA=`shasum < $ROOT_TEST_DIR/test/mobydick.txt | awk '{ print $1 }'`
  assert_equal $SHA `shasum < $BATS_TMPDIR/mobydick_test.txt | awk '{ print $1 }'`
}

@test "put dir into file" {
  run $HDFS put $ROOT_TEST_DIR/test /_test_cmd/put/existing.txt
    assert_failure
    assert_output <<OUT
mkdir /_test_cmd/put/existing.txt: file already exists
OUT
}

@test "put from stdin" {
  run bash -c "cat $ROOT_TEST_DIR/test/mobydick.txt | $HDFS put - /_test_cmd/put_stdin/mobydick_stdin.txt"
  assert_success

  run bash -c "$HDFS cat /_test_cmd/put_stdin/mobydick_stdin.txt > $BATS_TMPDIR/mobydick_stdin_test.txt"
  assert_success

  SHA=`shasum < $ROOT_TEST_DIR/test/mobydick.txt | awk '{ print $1 }'`
  assert_equal $SHA `shasum < $BATS_TMPDIR/mobydick_stdin_test.txt | awk '{ print $1 }'`
}

@test "put from stdin into existing file" {
  run bash -c "cat $ROOT_TEST_DIR/test/mobydick.txt | $HDFS put - /_test_cmd/put/existing.txt"
  assert_failure
  assert_output <<OUT
put /_test_cmd/put/existing.txt: file already exists
OUT
}

@test "put from stdin into dir" {
  run bash -c "cat $ROOT_TEST_DIR/test/mobydick.txt | $HDFS put - /_test_cmd/put/"
  assert_failure
  assert_output <<OUT
put /_test_cmd/put: file already exists
OUT
}

teardown() {
  $HDFS rm -r /_test_cmd/put
}
