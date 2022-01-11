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
			_, err := crud.GetBlockModel().SelectOne(uint32(currentBlockNumber))
			if errors.Is(err, gorm.ErrRecordNotFound) {
				blockMissing := &models.BlockMissing{
					Number: uint32(currentBlockNumber),
				}

				crud.GetBlockMissingModel().LoaderChannel <- blockMissing
			} else if err != nil {
				zap.S().Warn("Loader=BlockMissing Number=", currentBlockNumber, " Error=", err.Error(), " - Retrying...")

				time.Sleep(1 * time.Second)
				continue
			}

			currentBlockNumber++
		}

		zap.S().Info("Completed routine, sleeping...")
		time.Sleep(duration)
	}
}
