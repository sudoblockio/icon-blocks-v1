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
	consumerTopicChanLogs := kafka.KafkaTopicConsumers[consumerTopicNameLogs].TopicChannel

	// Output channels
	blockInternalTransactionChan := crud.GetBlockInternalTransactionModel().LoaderChannel

	zap.S().Debug("Logs Transformer: started working")
	for {
		// Read from kafka
		var consumerTopicMsg *sarama.ConsumerMessage

		consumerTopicMsg = <-consumerTopicChanLogs
		// Transaction message from ETL
		// NOTE: Only internal transactions
		logRaw, err := convertBytesToLogRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Info("Logs Transformer: Processing block #", logRaw.BlockNumber)
		if err != nil {
			zap.S().Fatal("Unable to proceed cannot convert kafka msg value to Log, err: ", err.Error())
		}

		// Create partial block from log
		blockInternalTransaction := transformLogRawToBlockInternalTransaction(logRaw)
		if blockInternalTransaction == nil {
			// Not an internal transaction
			continue
		}

		// Load to Postgres
		blockInternalTransactionChan <- blockInternalTransaction
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

func transformLogRawToBlockInternalTransaction(logRaw *models.LogRaw) *models.BlockInternalTransaction {

	//////////////////////////////////
	// Is log Internal Transaction? //
	//////////////////////////////////
	var indexed []string
	err := json.Unmarshal([]byte(logRaw.Indexed), &indexed)
	if err != nil {
		zap.S().Fatal("Unable to parse indexed field in log; indexed=", logRaw.Indexed, " error: ", err.Error())
	}
	method := strings.Split(indexed[0], "(")[0]
	if method != "ICXTransfer" {
		// Not internal transaction
		return nil
	}

	return &models.BlockInternalTransaction{
		Number:          uint32(logRaw.BlockNumber),
		TransactionHash: logRaw.TransactionHash,
		LogIndex:        uint32(logRaw.LogIndex),
		Amount:          indexed[3],
	}
}
