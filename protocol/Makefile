HDFS_PROTOS=ClientNamenodeProtocol.proto datatransfer.proto \
hdfs.proto acl.proto inotify.proto xattr.proto

HDFS_COMMON_PROTOS=IpcConnectionContext.proto \
ProtobufRpcEngine.proto RpcHeader.proto Security.proto

all:
	protoc --go_out=hadoop_hdfs/ $(HDFS_PROTOS)
	protoc --go_out=hadoop_common/ $(HDFS_COMMON_PROTOS)
	find . -name \*.pb.go -print | \
	  xargs grep -l 'import .* "."' | \
	  xargs -n1 sed -i~ -e '/^import _ "."/d' -e 's,import hadoop_common ".",import hadoop_common "github.com/colinmarc/hdfs/protocol/hadoop_common",'

clean:
	find . -name \*.pb.go -o -name \*~ -print | xargs rm -f
