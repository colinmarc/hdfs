set -e

hadoop fs -mkdir -p "/_test"
hadoop fs -chmod 777 "/_test"

hadoop fs -put ./testdata/foo.txt "/_test/foo.txt"
hadoop fs -Ddfs.block.size=1048576 -put ./testdata/mobydick.txt "/_test/mobydick.txt"
