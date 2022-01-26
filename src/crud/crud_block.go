package crud

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-blocks/models"
)

// BlockModel - type for block table model
type BlockModel struct {
	db            *gorm.DB
	model         *models.Block
	modelORM      *models.BlockORM
	LoaderChannel chan *models.Block
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
			db:            dbConn,
			model:         &models.Block{},
			modelORM:      &models.BlockORM{},
			LoaderChannel: make(chan *models.Block, 1),
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
// Returns: models, error (if present)
func (m *BlockModel) SelectMany(
	limit int,
	skip int,
	number uint32,
	startNumber uint32,
	endNumber uint32,
	hash string,
	createdBy string,
	sort string,
) (*[]models.BlockAPIList, error) {
	db := m.db

	// Latest blocks first
	if sort != "" {
		db = db.Order("number " + sort)
	}

	// Set table
	db = db.Model(&[]models.Block{})

	// Number
	if number != 0 {
		db = db.Where("number = ?", number)
	}

	// Start number and end number
	if startNumber != 0 && endNumber != 0 {
		db = db.Where("number BETWEEN ? AND ?", startNumber, endNumber)
	} else if startNumber != 0 {
		db = db.Where("number > ?", startNumber)
	} else if endNumber != 0 {
		db = db.Where("number < ?", endNumber)
	}

	// Hash
	if hash != "" {
		db = db.Where("hash = ?", hash)
	}

	// Created By (peer id)
	if createdBy != "" {
		db = db.Where("peer_id = ?", createdBy)
	}

	// Limit is required and defaulted to 1
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	blocks := &[]models.BlockAPIList{}
	db = db.Find(blocks)

	return blocks, db.Error
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

func (m *BlockModel) UpsertOne(
	block *models.Block,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*block),
		reflect.TypeOf(*block),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "number"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(block)

	return db.Error
}

// StartBlockLoader starts loader
func StartBlockLoader() {
	go func() {

		for {
			// Read block
			newBlock := <-GetBlockModel().LoaderChannel

			/////////////////
			// Enrichments //
			/////////////////
			transactionFees := ""
			transactionAmount := ""
			internalTransactionAmount := ""
			internalTransactionCount := 0
			failedTransactionCount := 0
			blockTime := uint64(0)

			////////////////////////
			// Block Transactions //
			////////////////////////
			allBlockTransactions, err := GetBlockTransactionModel().SelectMany(newBlock.Number)
			if err != nil {
				zap.S().Fatal(err.Error())
			}

			// transaction fee
			sumTransactionFeesBig := big.NewInt(0)
			for _, blockTransaction := range *allBlockTransactions {

				blockTransactionFeesBig := big.NewInt(0)
				blockTransactionFeesBig.SetString(blockTransaction.Fee[2:], 16)

				sumTransactionFeesBig = sumTransactionFeesBig.Add(sumTransactionFeesBig, blockTransactionFeesBig)
			}
			transactionFees = fmt.Sprintf("0x%x", sumTransactionFeesBig) // convert to hex

			// transaction amount
			sumTransactionAmountBig := big.NewInt(0)
			for _, blockTransaction := range *allBlockTransactions {

				blockTransactionAmountBig := big.NewInt(0)
				blockTransactionAmountBig.SetString(blockTransaction.Amount[2:], 16)

				sumTransactionAmountBig = sumTransactionAmountBig.Add(sumTransactionAmountBig, blockTransactionAmountBig)
			}
			transactionAmount = fmt.Sprintf("0x%x", sumTransactionAmountBig) // convert to hex

			/////////////////////////////////
			// Block Internal Transactions //
			/////////////////////////////////
			allBlockInternalTransactions, err := GetBlockInternalTransactionModel().SelectMany(newBlock.Number)
			if err != nil {
				zap.S().Fatal(err.Error())
			}

			// internal transaction amount
			sumInternalTransactionAmountBig := big.NewInt(0)
			for _, blockInternalTransaction := range *allBlockInternalTransactions {

				blockInternalTransactionAmountBig := big.NewInt(0)
				blockInternalTransactionAmountBig.SetString(blockInternalTransaction.Amount[2:], 16)

				sumInternalTransactionAmountBig = sumInternalTransactionAmountBig.Add(sumInternalTransactionAmountBig, blockInternalTransactionAmountBig)
			}
			internalTransactionAmount = fmt.Sprintf("0x%x", sumInternalTransactionAmountBig) // convert to hex

			// internal transaction count
			internalTransactionCount = len(*allBlockInternalTransactions)

			///////////////////////////////
			// Block Failed Transactions //
			///////////////////////////////
			allBlockFailedTransactions, err := GetBlockFailedTransactionModel().SelectMany(newBlock.Number)
			if err != nil {
				zap.S().Fatal(err.Error())
			}
			failedTransactionCount = len(*allBlockFailedTransactions)

			////////////////
			// Block Time //
			////////////////
			blockTimeRow, err := GetBlockTimeModel().SelectOne(newBlock.Number)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// No block_time entry yet
				blockTime = 0
			} else if err == nil {
				// Success
				blockTime = blockTimeRow.Time
			} else {
				// Postgres error
				zap.S().Fatal(err.Error())
			}

			newBlock.TransactionFees = transactionFees
			newBlock.TransactionAmount = transactionAmount
			newBlock.InternalTransactionAmount = internalTransactionAmount
			newBlock.InternalTransactionCount = uint32(internalTransactionCount)
			newBlock.FailedTransactionCount = uint32(failedTransactionCount)
			newBlock.BlockTime = blockTime

			//////////////////////
			// Load to postgres //
			//////////////////////
			err = GetBlockModel().UpsertOne(newBlock)
			zap.S().Debug("Loader=Block, Number=", newBlock.Number, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=Block, Number=", newBlock.Number, " - Error: ", err.Error())
			}
		}
	}()
}

// reloadBlock - Send block back to loader for updates
func reloadBlock(number uint32) error {

	curBlock, err := GetBlockModel().SelectOne(number)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// Create empty block
		curBlock = &models.Block{}
		curBlock.Number = number
	} else if err != nil {
		// Postgres error
		return err
	}
	GetBlockModel().LoaderChannel <- curBlock

	return nil
}
