package transformers

import (
	"go.uber.org/zap"
	"gopkg.in/Shopify/sarama.v1"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/kafka"
	"github.com/geometry-labs/icon-blocks/models"
	"github.com/geometry-labs/icon-blocks/worker/utils"
)

// StartBlocksTransformer - start block transformer go routine
func StartBlocksTransformer() {
	go blocksTransformer()
}

func blocksTransformer() {
	consumerTopicName := "blocks"
	producerTopicName := "blocks-ws"

	// Check topic names
	if utils.StringInSlice(consumerTopicName, config.Config.ConsumerTopics) == false {
		zap.S().Panic("Blocks Worker: no ", consumerTopicName, " topic found in CONSUMER_TOPICS=", config.Config.ConsumerTopics)
	}
	if utils.StringInSlice(producerTopicName, config.Config.ProducerTopics) == false {
		zap.S().Panic("Blocks Worker: no ", producerTopicName, " topic found in PRODUCER_TOPICS=", config.Config.ConsumerTopics)
	}

	consumerTopicChan := make(chan *sarama.ConsumerMessage)
	producerTopicChan := kafka.KafkaTopicProducers[producerTopicName].TopicChan
	postgresLoaderChan := crud.GetBlockModel().WriteChan

	// Register consumer channel
	broadcasterOutputChanID := kafka.Broadcasters[consumerTopicName].AddBroadcastChannel(consumerTopicChan)
	defer func() {
		kafka.Broadcasters[consumerTopicName].RemoveBroadcastChannel(broadcasterOutputChanID)
	}()

	zap.S().Debug("Blocks Worker: started working")
	for {
		// Read from kafka
		consumerTopicMsg := <-consumerTopicChan
		blockRaw, err := models.ConvertToBlockRawJSON(consumerTopicMsg.Value)
		if err != nil {
			zap.S().Fatal("BLOCKS TRANSFORMER PANIC: Unable to proceed cannot convert kafka msg value to Block")
		}

		// Transform logic
		transformedBlock, _ := transform(blockRaw)

		// Produce to Kafka
		producerTopicMsg := &sarama.ProducerMessage{
			Topic: producerTopicName,
			Key:   sarama.ByteEncoder(consumerTopicMsg.Key),
			Value: sarama.ByteEncoder(consumerTopicMsg.Value),
		}

		producerTopicChan <- producerTopicMsg

		// Load to Postgres
		postgresLoaderChan <- transformedBlock

		zap.S().Debug("Blocks worker: last seen block #", string(consumerTopicMsg.Key))
	}
}

// Business logic goes here
func transform(blocRaw *models.BlockRaw) (*models.Block, error) {
	//time.Sleep(time.Minute)
	return &models.Block{
		Signature:        blocRaw.Signature,
		ItemId:           blocRaw.ItemId,
		NextLeader:       blocRaw.NextLeader,
		TransactionCount: blocRaw.TransactionCount,
		Type:             blocRaw.Type,
		Version:          blocRaw.Version,
		PeerId:           blocRaw.PeerId,
		Number:           blocRaw.Number,
		MerkleRootHash:   blocRaw.MerkleRootHash,
		ItemTimestamp:    blocRaw.ItemTimestamp,
		Hash:             blocRaw.Hash,
		ParentHash:       blocRaw.ParentHash,
		Timestamp:        blocRaw.Timestamp,
	}, nil
}
