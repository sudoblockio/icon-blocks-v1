package routines

import (
	"time"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/models"
	"github.com/geometry-labs/icon-blocks/redis"
)

func StartBlockCountRoutine() {

	// routine every day
	go blockCountRoutine(3600 * time.Second)
}

func blockCountRoutine(duration time.Duration) {

	// Loop every duration
	for {

		/////////////
		// Regular //
		/////////////

		// Count
		count, err := crud.GetBlockCountIndexModel().Count()
		if err != nil {
			// Postgres error
			zap.S().Warn(err)
			continue
		}

		// Update Redis
		countKey := "icon_blocks_block_count_block"
		err = redis.GetRedisClient().SetCount(countKey, count)
		if err != nil {
			// Redis error
			zap.S().Warn(err)
			continue
		}

		// Update Postgres
		blockCount := &models.BlockCount{
			Type:  "block",
			Count: uint64(count),
		}
		err = crud.GetBlockCountModel().UpsertOne(blockCount)

		zap.S().Info("Completed routine, sleeping...")
		time.Sleep(duration)
	}
}
