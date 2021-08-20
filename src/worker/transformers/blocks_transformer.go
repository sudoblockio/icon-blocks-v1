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
	"github.com/geometry-labs/icon-blocks/worker/utils"
)

// StartBlocksTransformer - start block transformer go routine
func StartBlocksTransformer() {
	go blocksTransformer()
}

func blocksTransformer() {
	consumerTopicNameBlocks := "blocks"

	// Check topic names
	if utils.StringInSlice(consumerTopicNameBlocks, config.Config.ConsumerTopics) == false {
		zap.S().Panic("No ", consumerTopicNameBlocks, " topic found in CONSUMER_TOPICS=", config.Config.ConsumerTopics)
	}

	// Input channels
	consumerTopicChanBlocks := make(chan *sarama.ConsumerMessage)

	// Output channels
	blockLoaderChan := crud.GetBlockModel().WriteChan
	blockCountLoaderChan := crud.GetBlockCountModel().WriteChan
	redisClient := redis.GetRedisClient()

	// Register Input channel
	broadcasterOutputChanIDBlocks := kafka.Broadcasters[consumerTopicNameBlocks].AddBroadcastChannel(consumerTopicChanBlocks)
	defer func() {
		kafka.Broadcasters[consumerTopicNameBlocks].RemoveBroadcastChannel(broadcasterOutputChanIDBlocks)
	}()

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
		block = transformBlock(blockRaw)

		// Load block counter to Postgres
		blockCount := &models.BlockCount{
			Count: 1, // Adds with current
			Id:    1, // Only one row
		}
		blockCountLoaderChan <- blockCount

		// Push to redis
		blockJSON, _ := convertBlockToJSON(block)
		redisClient.Publish(blockJSON)

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

func convertBlockToJSON(block *models.Block) ([]byte, error) {
	data, err := json.Marshal(block)
	if err != nil {
		zap.S().Error("ConvertBlockToBytes ERROR:", err.Error())
	}

	return data, err
}

func transformBlock(blockRaw *models.BlockRaw) *models.Block {

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
