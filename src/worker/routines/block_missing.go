package routines

import (
	"errors"
	"time"

	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func StartBlockMissingRoutine() {

	// routine every day
	go blockMissingRoutine(3600 * time.Second)
}

func blockMissingRoutine(duration time.Duration) {

	// Loop every duration
	for {

		currentBlockNumber := 1

		for {
			block, err := crud.GetBlockModel().SelectOne(uint32(currentBlockNumber))
			if errors.Is(err, gorm.ErrRecordNotFound) || block.Hash == "" {
				blockMissing := &models.BlockMissing{
					Number: uint32(currentBlockNumber),
				}

				crud.GetBlockMissingModel().LoaderChannel <- blockMissing
			} else if err != nil {
				zap.S().Warn("Loader=BlockMissing Number=", currentBlockNumber, " Error=", err.Error(), " - Retrying...")

				time.Sleep(1 * time.Second)
				continue
			}

			// Block is within an hour from current time
			if (block.Timestamp / 1000000) > (time.Now().Unix() - 360) {
				break
			}

			if currentBlockNumber%100000 == 0 {
				zap.S().Info("Routine=BlockMissing, CurrentBlockNumber= ", currentBlockNumber, " - Checked 100,000 blocks...")
			}

			currentBlockNumber++
		}

		zap.S().Info("Routine=BlockMissing - Completed routine, sleeping...")
		time.Sleep(duration)
	}
}
