#!/bin/sh

set -e
cd $(dirname $0)

case $1 in
  cdh5)
    ./travis-setup-cdh5.sh
    ;;
  cdh6)
    ./travis-setup-cdh6.sh
    ;;
  hdp2)
    ./travis-setup-hdp2.sh
    ;;
  *)
    echo "Uknown platform: $PLATFORM"
    exit 1
    ;;
esac

./fixtures.sh
