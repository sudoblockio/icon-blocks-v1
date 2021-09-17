package transformers

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/kafka"
	"github.com/geometry-labs/icon-blocks/models"
)

// StartBlocksTransformer - start block transformer go routine
func StartBlocksTransformer() {
	go blocksTransformer()
}

func blocksTransformer() {
	consumerTopicNameBlocks := config.Config.ConsumerTopicBlocks

	// Input channels
	consumerTopicChanBlocks := kafka.KafkaTopicConsumers[consumerTopicNameBlocks].TopicChan

	// Output channels
	blockLoaderChan := crud.GetBlockModel().WriteChan
	blockWebsocketLoaderChan := crud.GetBlockWebsocketIndexModel().WriteChan
	blockCountLoaderChan := crud.GetBlockCountModel().WriteChan

	zap.S().Debug("Blocks transformer: started working")
	for {
		// Read from kafka
		consumerTopicMsg := <-consumerTopicChanBlocks
		// Block message from ETL
		blockRaw, err := convertToBlockRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Info("Blocks Transformer: Processing block #", blockRaw.Number)
		if err != nil {
			zap.S().Fatal("Blocks transformer: Unable to proceed cannot convert kafka msg value to BlockRaw, err: ", err.Error())
		}

		// Load to: blocks
		block := transformBlockRawToBlock(blockRaw)
		blockLoaderChan <- block

		// Load to: blocks
		blockWebsocket := transformBlockToBlockWS(block)
		blockWebsocketLoaderChan <- blockWebsocket

		// Load to: block_counts
		blockCount := transformBlockToBlockCount(block)
		blockCountLoaderChan <- blockCount

	}
}

func convertToBlockRawProtoBuf(value []byte) (*models.BlockRaw, error) {
	block := models.BlockRaw{}
	err := proto.Unmarshal(value[6:], &block)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
	}
	return &block, err
}

func transformBlockRawToBlock(blockRaw *models.BlockRaw) *models.Block {

	return &models.Block{
		Signature:                 blockRaw.Signature,
		ItemId:                    blockRaw.ItemId,
		NextLeader:                blockRaw.NextLeader,
		TransactionCount:          blockRaw.TransactionCount,
		Type:                      blockRaw.Type,
		Version:                   blockRaw.Version,
		PeerId:                    blockRaw.PeerId,
		Number:                    blockRaw.Number,
		MerkleRootHash:            blockRaw.MerkleRootHash,
		ItemTimestamp:             blockRaw.ItemTimestamp,
		Hash:                      blockRaw.Hash,
		ParentHash:                blockRaw.ParentHash,
		Timestamp:                 blockRaw.Timestamp,
		TransactionFees:           "0x0", // Adds in loader
		TransactionAmount:         "0x0", // Adds in loader
		InternalTransactionAmount: "0x0", // Adds in loader
		InternalTransactionCount:  0,     // Adds in loader
		FailedTransactionCount:    0,     // Adds in loader
	}
}

func transformBlockToBlockWS(block *models.Block) *models.BlockWebsocket {

	return &models.BlockWebsocket{
		TransactionCount: block.TransactionCount,
		Number:           block.Number,
		Hash:             block.Hash,
		Timestamp:        block.Timestamp,
	}
}

func transformBlockToBlockCount(block *models.Block) *models.BlockCount {

	return &models.BlockCount{
		Number: block.Number,
	}
}
