package rpc

import (
	"code.google.com/p/goprotobuf/proto"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
)

// pipelineManager mediates communication with the namenode in the context of
// a block write.
type pipelineManager struct {
	namenode *NamenodeConnection
	block    *hdfs.LocatedBlockProto
}

func newPipelineManager(namenode *NamenodeConnection, block *hdfs.LocatedBlockProto) *pipelineManager {
	return &pipelineManager{namenode: namenode, block: block}
}

// func (pm *pipelineManager) setupPipeline() {
// }

// func (pm *pipelineManager) updatePipelineWithoutDatanode(address string) {

// }

func (pm *pipelineManager) currentPipeline() []*hdfs.DatanodeInfoProto {
	// targets := make([]*hdfs.DatanodeInfoProto, 0, len(br.pipeline))
	// for _, loc := range s.block.GetLocs() {
	// 	addr := getDatanodeAddress(loc)
	// 	for _, pipelineAddr := range br.pipeline {
	// 		if ipAddr == addr {
	// 			append(targets, loc)
	// 		}
	// 	}
	// }
	//
	// return targets

	return pm.block.GetLocs()
}

func (pm *pipelineManager) currentStage() hdfs.OpWriteBlockProto_BlockConstructionStage {
	return hdfs.OpWriteBlockProto_PIPELINE_SETUP_CREATE // or PIPELINE_SETUP_STREAMING_RECOVERY for recovery
}

func (pm *pipelineManager) generationTimestamp() int64 {
	return 0
}

func (pm *pipelineManager) finalizeBlock(length int64) error {
	pm.block.GetB().NumBytes = proto.Uint64(uint64(length))
	updateReq := &hdfs.UpdateBlockForPipelineRequestProto{
		Block:      pm.block.GetB(),
		ClientName: proto.String(ClientName),
	}
	updateResp := &hdfs.UpdateBlockForPipelineResponseProto{}

	err := pm.namenode.Execute("updateBlockForPipeline", updateReq, updateResp)
	if err != nil {
		return err
	}

	return nil
}
