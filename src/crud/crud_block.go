package crud

import (
	"errors"
	"strings"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/models"
)

// BlockModel - type for block table model
type BlockModel struct {
	db        *gorm.DB
	model     *models.Block
	modelORM  *models.BlockORM
	WriteChan chan *models.Block
}

var blockModel *BlockModel
var blockModelOnce sync.Once

// GetBlockModel - create and/or return the blocks table model
func GetBlockModel() *BlockModel {
	blockModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		blockModel = &BlockModel{
			db:        dbConn,
			model:     &models.Block{},
			WriteChan: make(chan *models.Block, 1),
		}

		err := blockModel.Migrate()
		if err != nil {
			zap.S().Fatal("BlockModel: Unable migrate postgres table: ", err.Error())
		}

		StartBlockLoader()
	})

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
		query := m.db.Create(block)
		if query.Error != nil && !strings.Contains(query.Error.Error(), "duplicate key value violates unique constraint") {
			zap.S().Warn("POSTGRES Insert Error : ", query.Error.Error())
			return query.Error
		}

		return nil
	}, backoff.NewExponentialBackOff())

	return err
}

// SelectMany - select from blocks table
// Returns: models, total count (if filters), error (if present)
func (m *BlockModel) SelectMany(
	limit int,
	skip int,
	number uint32,
	startNumber uint32,
	endNumber uint32,
	hash string,
	createdBy string,
) (*[]models.BlockAPI, int64, error) {
	db := m.db
	computeCount := false

	// Latest blocks first
	db = db.Order("number desc")

	// Set table
	db = db.Model(&[]models.Block{})

	// Number
	if number != 0 {
		computeCount = true
		db = db.Where("number = ?", number)
	}

	// Start number and end number
	if startNumber != 0 && endNumber != 0 {
		computeCount = true
		db = db.Where("number BETWEEN ? AND ?", startNumber, endNumber)
	} else if startNumber != 0 {
		computeCount = true
		db = db.Where("number > ?", startNumber)
	} else if endNumber != 0 {
		computeCount = true
		db = db.Where("number < ?", endNumber)
	}

	// Hash
	if hash != "" {
		computeCount = true
		db = db.Where("hash = ?", hash[2:])
	}

	// Created By (peer id)
	if createdBy != "" {
		computeCount = true
		db = db.Where("peer_id = ?", createdBy)
	}

	// Count, if needed
	count := int64(-1)
	if computeCount {
		db.Count(&count)
	}

	// Limit is required and defaulted to 1
	// NOTE: Count before setting limit
	db = db.Limit(limit)

	// Skip
	// NOTE: Count before setting skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	blocks := &[]models.BlockAPI{}
	db = db.Find(blocks)

	return blocks, count, db.Error
}

// SelectOne - select from blocks table
func (m *BlockModel) SelectOne(
	number uint32,
) (*models.Block, error) {
	db := m.db

	db = db.Order("number desc")

	db = db.Where("number = ?", number)

	block := &models.Block{}
	db = db.First(block)

	return block, db.Error
}

// UpdateOne - select from blocks table
func (m *BlockModel) UpdateOne(
	block *models.Block,
) error {
	db := m.db

	db = db.Order("number desc")

	db = db.Where("number = ?", block.Number)

	db = db.Save(block)

	return db.Error
}

// StartBlockLoader starts loader
func StartBlockLoader() {
	go func() {

		for {
			// Read block
			newBlock := <-GetBlockModel().WriteChan

			/////////////////
			// Enrichments //
			/////////////////
			failedTransactionCount := 0
			internalTransactionCount := 0

			// Block Failed Transactions
			allBlockFailedTransactions, err := GetBlockFailedTransactionModel().SelectMany(newBlock.Number)
			if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}
			failedTransactionCount = len(*allBlockFailedTransactions)

			// Block Internal Transactions
			allBlockInternalTransactions, err := GetBlockInternalTransactionModel().SelectMany(newBlock.Number)
			if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}
			internalTransactionCount = len(*allBlockInternalTransactions)

			newBlock.FailedTransactionCount = uint32(failedTransactionCount)
			newBlock.InternalTransactionCount = uint32(internalTransactionCount)

			// Update/Insert
			_, err = GetBlockModel().SelectOne(newBlock.Number)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Insert
				GetBlockModel().Insert(newBlock)
			} else if err == nil {
				// Update
				GetBlockModel().UpdateOne(newBlock)
				zap.S().Debug("Loader=Block, Number=", newBlock.Number, " - Updated")
			} else {
				// Postgres error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}

// Send block back to loader for updates
func reloadBlock(number uint32) error {

	curBlock, err := GetBlockModel().SelectOne(number)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// Create empty block
		curBlock = &models.Block{}
		curBlock.Number = number
	} else if err != nil {
		// Postgress error
		return err
	}
	GetBlockModel().WriteChan <- curBlock

	return nil
}
