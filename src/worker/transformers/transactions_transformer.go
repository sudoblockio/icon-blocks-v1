package transformers

import (
	"fmt"
	"math/big"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/kafka"
	"github.com/geometry-labs/icon-blocks/metrics"
	"github.com/geometry-labs/icon-blocks/models"
)

// StartTransactionsTransformer - start block transformer go routine
func StartTransactionsTransformer() {
	go transactionsTransformer()
}

func transactionsTransformer() {
	consumerTopicNameTransactions := config.Config.ConsumerTopicTransactions

	// Input channels
	consumerTopicChanTransactions := kafka.KafkaTopicConsumers[consumerTopicNameTransactions].TopicChannel

	// Output channels
	blockTransactionLoaderChan := crud.GetBlockTransactionModel().LoaderChannel
	blockFailedTransactionLoaderChan := crud.GetBlockFailedTransactionModel().LoaderChannel

	zap.S().Debug("Transactions Transformer: started working")
	for {

		///////////////////
		// Kafka Message //
		///////////////////

		consumerTopicMsg := <-consumerTopicChanTransactions
		transactionRaw, err := convertBytesToTransactionRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Debug("Transactions Transformer: Processing block #", transactionRaw.BlockNumber)
		if err != nil {
			zap.S().Fatal("Unable to proceed cannot convert kafka msg value to TransactionRaw, err: ", err.Error())
		}

		/////////////
		// Loaders //
		/////////////

		// Loads to: block_transactions
		blockTransaction := transformTransactionRawToBlockTransaction(transactionRaw)
		blockTransactionLoaderChan <- blockTransaction

		// Loads to: block_failed_transactions
		blockFailedTransaction := transformTransactionRawToBlockFailedTransaction(transactionRaw)
		if blockFailedTransaction == nil {
			// Not a failed transaction
			continue
		}
		blockFailedTransactionLoaderChan <- blockFailedTransaction

		/////////////
		// Metrics //
		/////////////

		// max_block_number_transactions_raw
		metrics.MaxBlockNumberTransactionsRawGauge.Set(float64(transactionRaw.BlockNumber))
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

func transformTransactionRawToBlockFailedTransaction(transactionRaw *models.TransactionRaw) *models.BlockFailedTransaction {

	////////////////////////////
	// Is failed transaction? //
	////////////////////////////
	if transactionRaw.ReceiptStatus != 0 {
		// Not failed transaction
		return nil
	}

	return &models.BlockFailedTransaction{
		Number:          uint32(transactionRaw.BlockNumber),
		TransactionHash: transactionRaw.Hash,
	}
}

func transformTransactionRawToBlockTransaction(transactionRaw *models.TransactionRaw) *models.BlockTransaction {

	// Transaction fee calculation
	// Use big int
	// NOTE: transaction fees, once calculated (price*used) may be too large for postgres
	receiptStepPriceBig := big.NewInt(int64(transactionRaw.ReceiptStepPrice))
	receiptStepUsedBig := big.NewInt(int64(transactionRaw.ReceiptStepUsed))
	transactionFeesBig := receiptStepUsedBig.Mul(receiptStepUsedBig, receiptStepPriceBig)

	// to hex
	transactionFees := fmt.Sprintf("0x%x", transactionFeesBig)

	return &models.BlockTransaction{
		Number:          uint32(transactionRaw.BlockNumber),
		TransactionHash: transactionRaw.Hash,
		Fee:             transactionFees,      // Adds in loader
		Amount:          transactionRaw.Value, // Adds in loader
	}
}
