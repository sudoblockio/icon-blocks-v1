package transformers

import (
	"encoding/json"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gopkg.in/Shopify/sarama.v1"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/kafka"
	"github.com/geometry-labs/icon-blocks/models"
)

// StartLogsTransformer - start block transformer go routine
func StartLogsTransformer() {
	go logsTransformer()
}

func logsTransformer() {
	consumerTopicNameLogs := config.Config.ConsumerTopicLogs

	// Input channels
	consumerTopicChanLogs := kafka.KafkaTopicConsumers[consumerTopicNameLogs].TopicChan

	// Output channels
	blockLoaderChan := crud.GetBlockModel().WriteChan

	zap.S().Debug("Logs Transformer: started working")
	for {
		// Read from kafka
		var consumerTopicMsg *sarama.ConsumerMessage
		var block *models.Block

		consumerTopicMsg = <-consumerTopicChanLogs
		// Transaction message from ETL
		// NOTE: Only internal transactions
		logRaw, err := convertBytesToLogRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Info("Logs Transformer: Processing block #", logRaw.BlockNumber)
		if err != nil {
			zap.S().Fatal("Unable to proceed cannot convert kafka msg value to Log, err: ", err.Error())
		}

		// Create partial block from log
		block = transformLog(logRaw)
		if block == nil {
			// Not an internal transaction
			continue
		}

		// Load to Postgres
		blockLoaderChan <- block
	}
}

func convertBytesToLogRawProtoBuf(value []byte) (*models.LogRaw, error) {
	log := models.LogRaw{}
	err := proto.Unmarshal(value[6:], &log)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
	}
	return &log, err
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
		TransactionFees:          "0x0",      // Adds in loader
		TransactionAmount:        indexed[3], // Adds in loader
		InternalTransactionCount: 1,          // Adds in loader
		FailedTransactionCount:   0,          // Adds in loader
	}
}
