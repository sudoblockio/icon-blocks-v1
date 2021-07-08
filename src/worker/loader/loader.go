package loader

import (
	"fmt"
	"github.com/geometry-labs/icon-blocks/global"
	"github.com/geometry-labs/icon-blocks/models"
	"go.uber.org/zap"
)

func StartBlockLoader() {
	go BlockLoader()
}

func BlockLoader() {
	var block *models.Block
	postgresLoaderChan := global.GetGlobal().Blocks.GetWriteChan()
	for {
		block = <-postgresLoaderChan
		global.GetGlobal().Blocks.RetryCreate(block) // inserted here !!
		zap.S().Debug(fmt.Sprintf(
			"Loader Block: Loaded in postgres table Blocks, Block Number %d", block.Number),
		)
	}
}
