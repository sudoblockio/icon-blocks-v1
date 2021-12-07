package builders

import (
	"errors"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/models"
)

// Table builder for block_times
// Builds table 'block_times' from 'blocks'
func StartBlockTimeBuilder() {

	// Tail builder
	go startBlockTimeBuilder(1, 2)

	// Head builder
	// go startBlockTimeBuilder(1, 2)
}

func startBlockTimeBuilder(startParentBlockNumber uint32, startChildBlockNumber uint32) {

	parentBlockNumber := startParentBlockNumber
	childBlockNumber := startChildBlockNumber

	for {

		////////////////////////
		// Get blocks from DB //
		////////////////////////

		// Parent block
		parentBlock, err := crud.GetBlockModel().SelectOne(parentBlockNumber)
		if errors.Is(err, gorm.ErrRecordNotFound) || parentBlock.Hash == "" {
			// Block does not exist yet
			// Sleep and try again
			zap.S().Info("Builder=BlockTimeBuilder, BlockNumber=", parentBlockNumber, " - Block not seen yet. Sleeping 3 seconds...")

			time.Sleep(3 * time.Second)
			continue
		} else if err != nil {
			// Postgres error
			zap.S().Fatal(err.Error())
		}

		// Child block
		childBlock, err := crud.GetBlockModel().SelectOne(childBlockNumber)
		if errors.Is(err, gorm.ErrRecordNotFound) || childBlock.Timestamp == 0 {
			// Block does not exist yet
			// Sleep and try again
			zap.S().Info("Builder=BlockTimeBuilder, BlockNumber=", childBlockNumber, " - Block not seen yet. Sleeping 3 seconds...")

			time.Sleep(3 * time.Second)
			continue
		} else if err != nil {
			// Postgres error
			zap.S().Fatal(err.Error())
		}

		/////////////
		// Compute //
		/////////////
		blockTime := childBlock.Timestamp - parentBlock.Timestamp

		////////////////////////////
		// Load to block_times DB //
		////////////////////////////
		crud.GetBlockTimeModel().LoaderChannel <- &models.BlockTime{
			Number: childBlockNumber,
			Time:   blockTime,
		}

		///////////////
		// Increment //
		///////////////
		parentBlockNumber++
		childBlockNumber++
	}
}
