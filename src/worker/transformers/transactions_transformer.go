package transformers

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gopkg.in/Shopify/sarama.v1"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/kafka"
	"github.com/geometry-labs/icon-blocks/models"
	"github.com/geometry-labs/icon-blocks/worker/utils"
)

// StartTransactionsTransformer - start block transformer go routine
func StartTransactionsTransformer() {
	go transactionsTransformer()
}

func transactionsTransformer() {
	consumerTopicNameTransactions := "transactions"

	// Check topic names
	if utils.StringInSlice(consumerTopicNameTransactions, config.Config.ConsumerTopics) == false {
		zap.S().Panic("No ", consumerTopicNameTransactions, " topic found in CONSUMER_TOPICS=", config.Config.ConsumerTopics)
	}

	// Input channels
	consumerTopicChanTransactions := make(chan *sarama.ConsumerMessage)

	// Output channels
	blockLoaderChan := crud.GetBlockModel().WriteChan

	// Register Input channel
	broadcasterOutputChanIDTransactions := kafka.Broadcasters[consumerTopicNameTransactions].AddBroadcastChannel(consumerTopicChanTransactions)
	defer func() {
		kafka.Broadcasters[consumerTopicNameTransactions].RemoveBroadcastChannel(broadcasterOutputChanIDTransactions)
	}()

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
	transactionFee := transactionRaw.ReceiptStepPrice * transactionRaw.ReceiptStepUsed

	// DEBUG
	zap.S().Debug("HASH: ", transactionRaw.Hash)

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
		TransactionFees:          transactionFee,                // Adds in loader
		TransactionAmount:        transactionRaw.Value,          // Adds in loader
		InternalTransactionCount: 0,                             // Adds in loader
		FailedTransactionCount:   uint32(failedTransationCount), // Adds in loader
	}
}
