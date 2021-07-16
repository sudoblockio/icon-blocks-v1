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

func StartBlocksTransformer() {
	go blocksTransformer()
}

func blocksTransformer() {
	consumer_topic_name := "blocks"
	producer_topic_name := "blocks-ws"

	// Check topic names
	if utils.StringInSlice(consumer_topic_name, config.Config.ConsumerTopics) == false {
		zap.S().Panic("Blocks Worker: no ", consumer_topic_name, " topic found in CONSUMER_TOPICS=", config.Config.ConsumerTopics)
	}
	if utils.StringInSlice(producer_topic_name, config.Config.ProducerTopics) == false {
		zap.S().Panic("Blocks Worker: no ", producer_topic_name, " topic found in PRODUCER_TOPICS=", config.Config.ConsumerTopics)
	}

	consumer_topic_chan := make(chan *sarama.ConsumerMessage)
	producer_topic_chan := kafka.KafkaTopicProducers[producer_topic_name].TopicChan
	postgresLoaderChan := crud.GetBlockModel().WriteChan

	// Register consumer channel
	broadcaster_output_chan_id := kafka.Broadcasters[consumer_topic_name].AddBroadcastChannel(consumer_topic_chan)
	defer func() {
		kafka.Broadcasters[consumer_topic_name].RemoveBroadcastChannel(broadcaster_output_chan_id)
	}()

	zap.S().Debug("Blocks Worker: started working")
	for {
		// Read from kafka
		consumer_topic_msg := <-consumer_topic_chan
		blockRaw, err := models.ConvertToBlockRaw(consumer_topic_msg.Value)
		if err != nil {
			zap.S().Error("Blocks Worker: Unable to proceed cannot convert kafka msg value to Block")
		}

		// Transform logic
		transformedBlock, _ := transform(blockRaw)

		// Produce to Kafka
		producer_topic_msg := &sarama.ProducerMessage{
			Topic: producer_topic_name,
			Key:   sarama.ByteEncoder(consumer_topic_msg.Key),
			Value: sarama.ByteEncoder(consumer_topic_msg.Value),
		}

		producer_topic_chan <- producer_topic_msg

		// Load to Postgres
		postgresLoaderChan <- transformedBlock

		zap.S().Debug("Blocks worker: last seen block #", string(consumer_topic_msg.Key))
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
