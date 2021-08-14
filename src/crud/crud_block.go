package crud

import (
	"errors"
	"fmt"
	"math/big"
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
func (m *BlockModel) SelectMany(
	limit int,
	skip int,
	number uint32,
	startNumber uint32,
	endNumber uint32,
	hash string,
	createdBy string,
) ([]models.BlockAPI, error) {
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
		db = db.Where("number BETWEEN ? AND ?", startNumber, endNumber)
	} else if startNumber != 0 {
		db = db.Where("number > ?", startNumber)
	} else if endNumber != 0 {
		db = db.Where("number < ?", endNumber)
	}

	// Hash
	if hash != "" {
		db = db.Where("hash = ?", hash[2:])
	}

	// Created By (peer id)
	if createdBy != "" {
		db = db.Where("peer_id = ?", createdBy)
	}

	// Set table
	db = db.Model(&[]models.Block{})

	blocks := []models.BlockAPI{}
	db = db.Find(&blocks)

	return blocks, db.Error
}

// SelectOne - select from blocks table
func (m *BlockModel) SelectOne(
	number uint32,
) (models.Block, error) {
	db := m.db

	// Height
	if number != 0 {
		db = db.Where("number = ?", number)
	}

	block := models.Block{}
	db = db.First(&block)

	return block, db.Error
}

// UpdateOne - select from blocks table
func (m *BlockModel) UpdateOne(
	block *models.Block,
) error {
	db := m.db

	db = db.Where("number = ?", block.Number)

	db = db.Save(&block)

	return db.Error
}

func (m *BlockModel) CountAll() int64 {
	db := m.db

	var count int64
	db.Model(&[]models.Block{}).Count(&count)

	return count
}

// StartBlockLoader starts loader
func StartBlockLoader() {
	go func() {

		for {
			// Read block
			newBlock := <-GetBlockModel().WriteChan

			// Select
			currentBlock, err := GetBlockModel().SelectOne(newBlock.Number)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// No entry; Load block to database
				GetBlockModel().Insert(newBlock)

				zap.S().Debugf("Loader Block: Loaded in postgres table Blocks, Block Number %d", newBlock.Number)
				continue
			} else if err != nil {
				// Postgres error
				zap.S().Fatal("Loader Block ERROR: ", err.Error())
			}

			//  exists; update current entry
			var sumBlock *models.Block
			if newBlock.Type == "block" {
				// New entry has all feilds
				sumBlock = createSumBlock(newBlock, &currentBlock)
			} else if currentBlock.Type == "block" {
				// Current entry has all fields
				sumBlock = createSumBlock(&currentBlock, newBlock)
			} else {
				// No entries have all fields
				sumBlock = createSumBlock(newBlock, &currentBlock)
			}

			// Update current entry
			GetBlockModel().UpdateOne(sumBlock)
		}
	}()
}

func createSumBlock(baseBlock, newBlock *models.Block) *models.Block {
	//////////////////////
	// Transaction Fees //
	//////////////////////
	transactionFees := baseBlock.TransactionFees + newBlock.TransactionFees

	////////////////////////
	// Transaction Amount //
	////////////////////////
	newAmount := new(big.Int)
	curAmount := new(big.Int)

	newAmount.SetString(baseBlock.TransactionAmount, 16)
	curAmount.SetString(newBlock.TransactionAmount, 16)

	// big.Int
	sumAmount := new(big.Int)
	sumAmount.Add(newAmount, curAmount)

	// Hex string
	transactionAmount := fmt.Sprintf("%x", sumAmount)

	//////////////////////////
	// Internal Transaction //
	//////////////////////////
	internalTransactionCount := baseBlock.InternalTransactionCount + newBlock.InternalTransactionCount

	//////////////////////////////
	// Failed Transaction Count //
	//////////////////////////////
	failedTransactionCount := baseBlock.FailedTransactionCount + newBlock.FailedTransactionCount

	sumBlock := &models.Block{
		Signature:                baseBlock.Signature,
		ItemId:                   baseBlock.ItemId,
		NextLeader:               baseBlock.NextLeader,
		TransactionCount:         baseBlock.TransactionCount,
		Type:                     baseBlock.Type,
		Version:                  baseBlock.Version,
		PeerId:                   baseBlock.PeerId,
		Number:                   baseBlock.Number,
		MerkleRootHash:           baseBlock.MerkleRootHash,
		ItemTimestamp:            baseBlock.ItemTimestamp,
		Hash:                     baseBlock.Hash,
		ParentHash:               baseBlock.ParentHash,
		Timestamp:                baseBlock.Timestamp,
		TransactionFees:          transactionFees,
		TransactionAmount:        transactionAmount,
		InternalTransactionCount: internalTransactionCount,
		FailedTransactionCount:   failedTransactionCount,
	}

	return sumBlock
}
