#!/usr/bin/env bats

load helper

@test "te: upload via native client, ensure we can download" {
  if [ "$TRANSPARENT_ENCRYPTION" = "true" ]; then
    run $HADOOP_FS -put $ROOT_TEST_DIR/testdata/foo.txt /_test/kms/foo1
    assert_success
    run $HDFS cat /_test/kms/foo1
    assert_output "bar"
  else
    skip
  fi
}

@test "te: ensure native client can download once we uploaded to encrypted zone" {
  if [ "$TRANSPARENT_ENCRYPTION" = "true" ]; then
    run $HDFS put $ROOT_TEST_DIR/testdata/foo.txt /_test/kms/foo2
    assert_success
    run $HADOOP_FS -cat /_test/kms/foo2
    assert_output "bar"
  else
    skip
  fi
}

@test "te: tail" {
  if [ "$TRANSPARENT_ENCRYPTION" = "true" ]; then
    run $HDFS put $ROOT_TEST_DIR/testdata/mobydick.txt /_test/kms/
    assert_success
    run bash -c "$HDFS tail /_test/kms/mobydick.txt > $BATS_TMPDIR/mobydick_test.txt"
    assert_success
    SHA=`tail $ROOT_TEST_DIR/testdata/mobydick.txt | shasum | awk '{ print $1 }'`
    assert_equal $SHA `shasum < $BATS_TMPDIR/mobydick_test.txt | awk '{ print $1 }'`
  else
    skip
  fi
}

@test "te: key not available" {
  if [ "$TRANSPARENT_ENCRYPTION" = "true" ]; then
    run $HADOOP_FS -mkdir -p /_test/kms-no-key
    assert_success
    run $HADOOP_KEY create key-removed
    assert_success
    run hdfs crypto -createZone -keyName key-removed -path /_test/kms-no-key
    assert_success
    run $HADOOP_FS -put $ROOT_TEST_DIR/testdata/foo.txt /_test/kms-no-key/foo
    assert_success
    run $HADOOP_KEY delete key-removed -f
    assert_success
    run $HDFS cat /_test/kms-no-key/foo
    assert_failure
    assert_output "open /_test/kms-no-key/foo: kms: 'key-removed@0' not found"

    run $HDFS put $ROOT_TEST_DIR/testdata/foo.txt /_test/kms-no-key/foo2
    assert_failure
    assert_output "create /_test/kms-no-key/foo2: kms: 'key-removed@0' not found"

    run $HDFS ls /_test/kms-no-key/foo2
    assert_failure
    assert_output "stat /_test/kms-no-key/foo2: file does not exist"
  else
    skip
  fi
}
