package loader

import (
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/models"
)

func StartBlockLoader() {
	go BlockLoader()
}

func BlockLoader() {

	var block *models.Block
	postgresLoaderChan := crud.GetBlockModel().WriteChan

	for {
    // Read block
		block = <-postgresLoaderChan

    // Load block to database
		crud.GetBlockModel().Create(block)

		zap.S().Debugf("Loader Block: Loaded in postgres table Blocks, Block Number %d", block.Number)
  }
}
