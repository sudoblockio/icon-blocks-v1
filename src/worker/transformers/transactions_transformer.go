package transformers

import (
	"fmt"
	"math/big"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gopkg.in/Shopify/sarama.v1"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/kafka"
	"github.com/geometry-labs/icon-blocks/models"
)

// StartTransactionsTransformer - start block transformer go routine
func StartTransactionsTransformer() {
	go transactionsTransformer()
}

func transactionsTransformer() {
	consumerTopicNameTransactions := config.Config.ConsumerTopicNameTransactions

	// Input channels
	consumerTopicChanTransactions := kafka.KafkaTopicConsumers[consumerTopicNameTransactions].TopicChan

	// Output channels
	blockLoaderChan := crud.GetBlockModel().WriteChan

	zap.S().Debug("Transactions Transformer: started working")
	for {
		// Read from kafka
		var consumerTopicMsg *sarama.ConsumerMessage
		var block *models.Block

		consumerTopicMsg = <-consumerTopicChanTransactions
		// Transaction message from ETL
		// Regular transactions
		transactionRaw, err := convertBytesToTransactionRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Debug("Transactions Transformer: Processing block #", transactionRaw.BlockNumber)
		if err != nil {
			zap.S().Fatal("Unable to proceed cannot convert kafka msg value to TransactionRaw, err: ", err.Error())
		}

		// Create partial block from transaction
		block = transformTransaction(transactionRaw)

		// Load to Postgres
		blockLoaderChan <- block
	}
}

func convertBytesToTransactionRawProtoBuf(value []byte) (*models.TransactionRaw, error) {
	tx := models.TransactionRaw{}
	err := proto.Unmarshal(value[6:], &tx)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
	}
	return &tx, err
}

func transformTransaction(transactionRaw *models.TransactionRaw) *models.Block {

	// Is failed transaction?
	failedTransationCount := 0
	if transactionRaw.ReceiptStatus == 0 {
		failedTransationCount = 1
	}

	// Transaction fee calculation
	// Use big int
	// NOTE: transaction fees, once calculated (price*used) may be too large for postgres
	receiptStepPriceBig := big.NewInt(int64(transactionRaw.ReceiptStepPrice))
	receiptStepUsedBig := big.NewInt(int64(transactionRaw.ReceiptStepUsed))
	transactionFeesBig := receiptStepUsedBig.Mul(receiptStepUsedBig, receiptStepPriceBig)

	// to hex
	transactionFees := fmt.Sprintf("0x%x", transactionFeesBig)

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
		TransactionFees:          transactionFees,               // Adds in loader
		TransactionAmount:        transactionRaw.Value,          // Adds in loader
		InternalTransactionCount: 0,                             // Adds in loader
		FailedTransactionCount:   uint32(failedTransationCount), // Adds in loader
	}
}
