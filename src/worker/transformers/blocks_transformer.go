package transformers

import (
	"encoding/hex"
	"encoding/json"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
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
	consumerTopicNameBlocks := "blocks"
	consumerTopicNameTransactions := "transactions"
	consumerTopicNameLogs := "logs"
	producerTopicName := "blocks-ws"

	// Check topic names
	if utils.StringInSlice(consumerTopicNameBlocks, config.Config.ConsumerTopics) == false {
		zap.S().Panic("Blocks Worker: no ", consumerTopicNameBlocks, " topic found in CONSUMER_TOPICS=", config.Config.ConsumerTopics)
	}
	if utils.StringInSlice(consumerTopicNameTransactions, config.Config.ConsumerTopics) == false {
		zap.S().Panic("Blocks Worker: no ", consumerTopicNameTransactions, " topic found in CONSUMER_TOPICS=", config.Config.ConsumerTopics)
	}
	if utils.StringInSlice(consumerTopicNameLogs, config.Config.ConsumerTopics) == false {
		zap.S().Panic("Blocks Worker: no ", consumerTopicNameLogs, " topic found in CONSUMER_TOPICS=", config.Config.ConsumerTopics)
	}
	if utils.StringInSlice(producerTopicName, config.Config.ProducerTopics) == false {
		zap.S().Panic("Blocks Worker: no ", producerTopicName, " topic found in PRODUCER_TOPICS=", config.Config.ConsumerTopics)
	}

	consumerTopicChanBlocks := make(chan *sarama.ConsumerMessage)
	consumerTopicChanTransactions := make(chan *sarama.ConsumerMessage)
	consumerTopicChanLogs := make(chan *sarama.ConsumerMessage)
	producerTopicChan := kafka.KafkaTopicProducers[producerTopicName].TopicChan
	postgresLoaderChan := crud.GetBlockModel().WriteChan

	// Register consumer channel
	broadcasterOutputChanIDBlocks := kafka.Broadcasters[consumerTopicNameBlocks].AddBroadcastChannel(consumerTopicChanBlocks)
	defer func() {
		kafka.Broadcasters[consumerTopicNameBlocks].RemoveBroadcastChannel(broadcasterOutputChanIDBlocks)
	}()
	broadcasterOutputChanIDTransactions := kafka.Broadcasters[consumerTopicNameTransactions].AddBroadcastChannel(consumerTopicChanTransactions)
	defer func() {
		kafka.Broadcasters[consumerTopicNameTransactions].RemoveBroadcastChannel(broadcasterOutputChanIDTransactions)
	}()
	broadcasterOutputChanIDLogs := kafka.Broadcasters[consumerTopicNameLogs].AddBroadcastChannel(consumerTopicChanLogs)
	defer func() {
		kafka.Broadcasters[consumerTopicNameLogs].RemoveBroadcastChannel(broadcasterOutputChanIDLogs)
	}()

	zap.S().Debug("Blocks Worker: started working")
	for {
		// Read from kafka
		var consumerTopicMsg *sarama.ConsumerMessage
		var block *models.Block

		select {
		case consumerTopicMsg = <-consumerTopicChanBlocks:
			// Block message from ETL
			blockRaw, err := convertToBlockRawProtoBuf(consumerTopicMsg.Value)
			if err != nil {
				zap.S().Fatal("Transactions Worker: Unable to proceed cannot convert kafka msg value to BlockRaw, err: ", err.Error())
			}

			// Transform logic
			block = transformBlock(blockRaw)

		case consumerTopicMsg = <-consumerTopicChanTransactions:
			// Transaction message from ETL
			// Regular transactions
			transactionRaw, err := convertBytesToTransactionRawProtoBuf(consumerTopicMsg.Value)
			if err != nil {
				zap.S().Fatal("Unable to proceed cannot convert kafka msg value to TransactionRaw, err: ", err.Error())
			}

			// Create partial block from transaction
			block = transformTransaction(transactionRaw)

		case consumerTopicMsg = <-consumerTopicChanLogs:
			// Transaction message from ETL
			// Internal Transactions
			logRaw, err := convertBytesToLogRawProtoBuf(consumerTopicMsg.Value)
			if err != nil {
				zap.S().Fatal("Unable to proceed cannot convert kafka msg value to Log, err: ", err.Error())
			}

			// Create partial block from log
			block = transformLog(logRaw)
			if block == nil {
				// Not an internal transaction
				continue
			}
		}
		// Produce to Kafka
		producerTopicMsg := &sarama.ProducerMessage{
			Topic: producerTopicName,
			Key:   sarama.ByteEncoder(consumerTopicMsg.Key),
			Value: sarama.ByteEncoder(consumerTopicMsg.Value),
		}
		producerTopicChan <- producerTopicMsg

		// Load to Postgres
		postgresLoaderChan <- block

		zap.S().Debug("Blocks worker: last seen block #", string(consumerTopicMsg.Key))
	}
}

func convertToBlockRawProtoBuf(value []byte) (*models.BlockRaw, error) {
	block := models.BlockRaw{}
	err := proto.Unmarshal(value[6:], &block)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
		zap.S().Error("Value=", hex.Dump(value[6:]))
	}
	return &block, err
}

func convertBytesToTransactionRawProtoBuf(value []byte) (*models.TransactionRaw, error) {
	tx := models.TransactionRaw{}
	err := proto.Unmarshal(value[6:], &tx)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
		zap.S().Error("Value=", hex.Dump(value[6:]))
	}
	return &tx, err
}

func convertBytesToLogRawProtoBuf(value []byte) (*models.LogRaw, error) {
	log := models.LogRaw{}
	err := proto.Unmarshal(value[6:], &log)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
		zap.S().Error("Value=", hex.Dump(value[6:]))
	}
	return &log, err
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
		TransactionFees:          0,
		TransactionAmount:        "",
		InternalTransactionCount: 0,
		FailedTransactionCount:   0,
	}
}

func transformTransaction(transactionRaw *models.TransactionRaw) *models.Block {

	// Is failed transaction?
	failedTransationCount := 0
	if transactionRaw.ReceiptStatus == 0 {
		failedTransationCount = 1
	}

	// Transaction fee calculation
	transactionFee := transactionRaw.ReceiptStepPrice * transactionRaw.ReceiptCumulativeStepUsed
	zap.S().Info("Height: ", transactionRaw.BlockNumber)
	zap.S().Info("Fee: ", transactionFee)

	// Represents a change of state
	// Linked by BlockNumber
	return &models.Block{
		Signature:                "",
		ItemId:                   "",
		NextLeader:               "",
		TransactionCount:         0,
		Type:                     "transaction",
		Version:                  "",
		PeerId:                   "",
		Number:                   uint32(transactionRaw.BlockNumber),
		MerkleRootHash:           "",
		ItemTimestamp:            "",
		Hash:                     transactionRaw.BlockHash,
		ParentHash:               "",
		Timestamp:                0,
		TransactionFees:          transactionFee,
		TransactionAmount:        transactionRaw.Value,
		InternalTransactionCount: 0,
		FailedTransactionCount:   uint32(failedTransationCount),
	}
}

func transformLog(logRaw *models.LogRaw) *models.Block {

	// Extract method
	var indexed []string
	err := json.Unmarshal([]byte(logRaw.Indexed), &indexed)
	if err != nil {
		zap.S().Fatal("Unable to parse indexed field in log; indexed=", logRaw.Indexed, " error: ", err.Error())
	}
	method := strings.Split(indexed[0], "(")[0]

	// Is interal transaction?
	if method != "ICXTransfer" {
		// Not internal transaction
		return nil
	}

	// Represents a change of state
	// Linked by BlockNumber
	return &models.Block{
		Signature:                "",
		ItemId:                   "",
		NextLeader:               "",
		TransactionCount:         0,
		Type:                     "log",
		Version:                  "",
		PeerId:                   "",
		Number:                   uint32(logRaw.BlockNumber),
		MerkleRootHash:           "",
		ItemTimestamp:            "",
		Hash:                     logRaw.BlockHash,
		ParentHash:               "",
		Timestamp:                0,
		TransactionFees:          0,
		TransactionAmount:        indexed[3],
		InternalTransactionCount: 1,
		FailedTransactionCount:   0,
	}
}
