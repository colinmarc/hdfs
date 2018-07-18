#!/usr/bin/env bats

load helper

@test "cat" {
  run $HDFS cat /_test/foo.txt
  assert_success
  assert_output "bar"
}

@test "cat long" {
  run bash -c "$HDFS cat /_test/mobydick.txt > $BATS_TMPDIR/mobydick_test.txt"
  assert_success

  SHA=`shasum < $ROOT_TEST_DIR/testdata/mobydick.txt | awk '{ print $1 }'`
  assert_equal $SHA `shasum < $BATS_TMPDIR/mobydick_test.txt | awk '{ print $1 }'`
}

@test "cat nonexistent" {
  run $HDFS cat /_test_cmd/nonexistent
  assert_failure
  assert_output <<OUT
open /_test_cmd/nonexistent: file does not exist
OUT
}
