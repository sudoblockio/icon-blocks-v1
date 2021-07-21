package crud

import (
	"strings"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/models"
)

var blockModel *struct {
	db        *gorm.DB
	model     *models.Block
	modelORM  *models.BlockORM
	WriteChan chan *models.Block
}

// GetBlockModel - create and/or return the blocks table model
func GetBlockModel() *BlockModel {
	if blockModel == nil {
		blockModel = &BlockModel{
			db:        getPostgresConn().conn,
			model:     &models.Block{},
			WriteChan: make(chan *models.Block, 1),
		}

		err := blockModel.Migrate()
		if err != nil {
			zap.S().Error("BlockModel: Unable create postgres table: blocks")
		}
	}

	return blockModel
}

// Migrate - migrate blocks table
func (m *BlockModel) Migrate() error {
	// Only using BlockRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert block into table
func (m *BlockModel) Insert(block *models.Block) error {

	err := backoff.Retry(func() error {
		query := m.db.Insert(block)
		if query.Error != nil && !strings.Contains(query.Error.Error(), "duplicate key value violates unique constraint") {
			zap.S().Warn("POSTGRES Insert Error : ", query.Error.Error())
			return query.Error
		}

		return nil
	}, backoff.NewExponentialBackOff())

	return err
}

// Select - select from blocks table
func (m *BlockModel) Select(
	limit int,
	skip int,
	number uint32,
	startNumber uint32,
	endNumber uint32,
	hash string,
	createdBy string,
) *[]models.Block {
	db := m.db

	// Latest blocks first
	db = db.Order("number desc")

	// Limit is required and defaulted to 1
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	// Height
	if number != 0 {
		db = db.Where("number = ?", number)
	}

	// Start number and end number
	if startNumber != 0 && endNumber != 0 {
		db = db.Where("number BETWEEN ? AND ?", start_number, endNumber)
	} else if startNumber != 0 {
		db = db.Where("number > ?", start_number)
	} else if endNumber != 0 {
		db = db.Where("number < ?", endNumber)
	}

	// Hash
	if hash != "" {
		db = db.Where("hash = ?", hash)
	}

	// Created By
	if createdBy != "" {
		db = db.Where("createdBy = ?", createdBy)
	}

	blocks := &[]models.Block{}
	db.Find(blocks)

	return blocks
}

// StartBlockLoader starts loader
func StartBlockLoader() {
	go func() {
		var block *models.Block
		postgresLoaderChan := GetBlockModel().WriteChan

		for {
			// Read block
			block = <-postgresLoaderChan

			// Load block to database
			GetBlockModel().Insert(block)

			zap.S().Debugf("Loader Block: Loaded in postgres table Blocks, Block Number %d", block.Number)
		}
	}()
}
