package builders

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/models"
	"github.com/geometry-labs/icon-blocks/redis"
)

// Table builder for block_times
// Builds table 'block_times' from 'blocks'
func StartBlockTransactionBuilder() {

	go startBlockTransactionBuilder(1, "_tail")

	go startBlockTransactionBuilder(45500000, "_head")
}

func startBlockTransactionBuilder(startBlockNumber, redisCounterSuffix) {

	// Query Redis for start block number
	countKey := "icon_blocks_block_transaction_builder_start_number" + redisCounterSuffix
	blockNumberRedis, err := redis.GetRedisClient().GetCount(countKey)
	if err != nil {
		zap.S().Fatal("Builder=BlockTransactionBuilder, Error: ", err.Error())
	} else if blockNumberRedis == -1 {
		// No blockNumber set yet
		blockNumberRedis = startBlockNumber
	}

	blockNumber := uint32(blockNumberRedis)
	for {
		zap.S().Debug("Builder=BlockTransactionBuilder, BlockNumber=", blockNumber, " - Computing...")

		//////////////
		// Query DB //
		//////////////

		block, err := crud.GetBlockModel().SelectOne(blockNumber)
		if errors.Is(err, gorm.ErrRecordNotFound) || block.Hash == "" {
			// Block does not exist yet
			// Sleep and try again
			zap.S().Info("Builder=BlockTransactionBuilder, BlockNumber=", blockNumber, " - Block not seen yet. Sleeping 1 second...")

			time.Sleep(1 * time.Second)
			continue
		} else if err != nil {
			// Postgres error
			zap.S().Fatal(err.Error())
		}

		transactions, err := crud.GetBlockTransactionModel().SelectMany(blockNumber)
		if errors.Is(err, gorm.ErrRecordNotFound) || len(*transactions) != int(block.TransactionCount) {
			// Transacitons do not exist yet
			// Sleep and try again
			zap.S().Info("Builder=BlockTransactionBuilder, BlockNumber=", blockNumber, " - Transactions not seen yet. Sleeping 1 second...")

			time.Sleep(1 * time.Second)
			continue
		} else if err != nil {
			// Postgres error
			zap.S().Fatal(err.Error())
		}

		/////////////
		// Compute //
		/////////////

		blockTransactionAmountBig := big.NewInt(0)
		blockTransactionFeesBig := big.NewInt(0)
		for _, t := range *transactions {

			// Transaction Amount
			transactionAmountBig := big.NewInt(0)
			transactionAmountBig.SetString(t.Amount[2:], 16)

			blockTransactionAmountBig = blockTransactionAmountBig.Add(blockTransactionAmountBig, transactionAmountBig)

			// Transaction Fees
			transactionFeeBig := big.NewInt(0)
			transactionFeeBig.SetString(t.Fee[2:], 16)

			blockTransactionFeesBig = blockTransactionFeesBig.Add(blockTransactionFeesBig, transactionFeeBig)
		}

		blockTransactionAmount := fmt.Sprintf("0x%x", blockTransactionAmountBig)
		blockTransactionFees := fmt.Sprintf("0x%x", blockTransactionFeesBig)

		/////////////
		// Load DB //
		/////////////
		crud.GetBlockModel().LoaderChannel <- &models.Block{
			Number:            block.Number,
			TransactionAmount: blockTransactionAmount,
			TransactionFees:   blockTransactionFees,
		}

		///////////////
		// Increment //
		///////////////
		blockNumber++

		if blockNumber % 100000 {
			zap.S().Info("Builder=BlockTransactionBuilder, BlockNumber=", blockNumber, " - Finished 100,000 blocks")
		}

		// Set count in redis
		err = redis.GetRedisClient().SetCount(countKey, int64(blockNumber))
		if err != nil {
			// Redis error
			zap.S().Fatal("Error: ", err.Error())
		}
	}
}
