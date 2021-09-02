package transformers

import (
	"encoding/json"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gopkg.in/Shopify/sarama.v1"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/kafka"
	"github.com/geometry-labs/icon-blocks/models"
	"github.com/geometry-labs/icon-blocks/redis"
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
	blockCountLoaderChan := crud.GetBlockCountModel().WriteChan
	redisClient := redis.GetRedisClient()

	zap.S().Debug("Blocks transformer: started working")
	for {
		// Read from kafka
		var consumerTopicMsg *sarama.ConsumerMessage
		var block *models.Block

		consumerTopicMsg = <-consumerTopicChanBlocks
		// Block message from ETL
		blockRaw, err := convertToBlockRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Info("Blocks Transformer: Processing block #", blockRaw.Number)
		if err != nil {
			zap.S().Fatal("Blocks transformer: Unable to proceed cannot convert kafka msg value to BlockRaw, err: ", err.Error())
		}

		// Transform logic
		block = transformBlockRawToBlock(blockRaw)

		// Load block counter to Postgres
		blockCount := &models.BlockCount{
			Count: 1, // Adds with current
			Id:    1, // Only one row
		}
		blockCountLoaderChan <- blockCount

		// Push to redis
		blockWebsocket := transformBlockToBlockWS(block)
		blockWebsocketJSON, _ := json.Marshal(blockWebsocket)
		redisClient.Publish(blockWebsocketJSON)

		// Load to Postgres
		blockLoaderChan <- block
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
		Signature:                blockRaw.Signature,
		ItemId:                   blockRaw.ItemId,
		NextLeader:               blockRaw.NextLeader,
		TransactionCount:         blockRaw.TransactionCount,
		Type:                     blockRaw.Type,
		Version:                  blockRaw.Version,
		PeerId:                   blockRaw.PeerId,
		Number:                   blockRaw.Number,
		MerkleRootHash:           blockRaw.MerkleRootHash,
		ItemTimestamp:            blockRaw.ItemTimestamp,
		Hash:                     blockRaw.Hash,
		ParentHash:               blockRaw.ParentHash,
		Timestamp:                blockRaw.Timestamp,
		TransactionFees:          "0x0", // Adds in loader
		TransactionAmount:        "0x0", // Adds in loader
		InternalTransactionCount: 0,     // Adds in loader
		FailedTransactionCount:   0,     // Adds in loader
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
