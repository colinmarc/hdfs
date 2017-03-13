#!/usr/bin/env bats

load helper

@test "df" {
  run $HDFS df
  assert_success
}
