package crud

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

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
// Returns: models, total count (if filters), error (if present)
func (m *BlockModel) SelectMany(
	limit int,
	skip int,
	number uint32,
	startNumber uint32,
	endNumber uint32,
	hash string,
	createdBy string,
) ([]models.BlockAPI, int64, error) {
	db := m.db
	computeCount := false

	// Latest blocks first
	db = db.Order("number desc")

	// Set table
	db = db.Model(&[]models.Block{})

	// Height
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

	blocks := []models.BlockAPI{}
	db = db.Find(&blocks)

	return blocks, count, db.Error
}

// SelectOne - select from blocks table
func (m *BlockModel) SelectOne(
	number uint32,
) (models.Block, error) {
	db := m.db

	db = db.Order("number desc")

	db = db.Where("number = ?", number)

	block := models.Block{}
	db = db.First(&block)

	return block, db.Error
}

// UpdateOne - select from blocks table
func (m *BlockModel) UpdateOne(
	block *models.Block,
) error {
	db := m.db

	db = db.Order("number desc")

	db = db.Where("number = ?", block.Number)

	db = db.Save(&block)

	return db.Error
}

// StartBlockLoader starts loader
func StartBlockLoader() {
	go func() {

		for {
			// Read block
			newBlock := <-GetBlockModel().WriteChan

			// Block to insert/update
			var sumBlock *models.Block

			// Select
			curBlock, err := GetBlockModel().SelectOne(newBlock.Number)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// No entry; Load block to database
				GetBlockModel().Insert(newBlock)

				// Set for checking later
				sumBlock = newBlock

				zap.S().Debugf("Inserted block number %d", newBlock.Number)
			} else if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			} else {
				// Exists; update current entry

				if newBlock.Type == "block" {
					// New entry has all feilds
					sumBlock = createSumBlock(newBlock, &curBlock)
				} else if curBlock.Type == "block" {
					// Current entry has all fields
					sumBlock = createSumBlock(&curBlock, newBlock)
				} else {
					// No entries have all fields
					sumBlock = createSumBlock(newBlock, &curBlock)
				}

				// Update current entry
				GetBlockModel().UpdateOne(sumBlock)
			}

			// Check current state
			for {
				// Wait for postgres to set state before processing more messages

				checkSumBlock, err := GetBlockModel().SelectOne(newBlock.Number)
				if err != nil {
					zap.S().Warn("State check error: ", err.Error())
					zap.S().Warn("Waiting 100ms...")
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// check all fields
				if sumBlock.Signature == checkSumBlock.Signature &&
					sumBlock.ItemId == checkSumBlock.ItemId &&
					sumBlock.NextLeader == checkSumBlock.NextLeader &&
					sumBlock.TransactionCount == checkSumBlock.TransactionCount &&
					sumBlock.Type == checkSumBlock.Type &&
					sumBlock.Version == checkSumBlock.Version &&
					sumBlock.PeerId == checkSumBlock.PeerId &&
					sumBlock.Number == checkSumBlock.Number &&
					sumBlock.MerkleRootHash == checkSumBlock.MerkleRootHash &&
					sumBlock.ItemTimestamp == checkSumBlock.ItemTimestamp &&
					sumBlock.Hash == checkSumBlock.Hash &&
					sumBlock.ParentHash == checkSumBlock.ParentHash &&
					sumBlock.Timestamp == checkSumBlock.Timestamp &&
					sumBlock.TransactionFees == checkSumBlock.TransactionFees &&
					sumBlock.TransactionAmount == checkSumBlock.TransactionAmount &&
					sumBlock.InternalTransactionCount == checkSumBlock.InternalTransactionCount &&
					sumBlock.FailedTransactionCount == checkSumBlock.FailedTransactionCount {
					// Success
					break
				} else {
					// Wait

					// DEBUG
					newBlockJSON, _ := json.Marshal(newBlock)
					curBlockJSON, _ := json.Marshal(curBlock)
					sumBlockJSON, _ := json.Marshal(sumBlock)
					checkSumBlockJSON, _ := json.Marshal(checkSumBlock)

					zap.S().Debug("newBlock: ", string(newBlockJSON))
					zap.S().Debug("curBlock: ", string(curBlockJSON))
					zap.S().Debug("sumBlock: ", string(sumBlockJSON))
					zap.S().Debug("cheBlock: ", string(checkSumBlockJSON))

					zap.S().Warn("Models did not match")
					zap.S().Warn("Waiting 100ms...")
					time.Sleep(100 * time.Millisecond)
					continue
				}
			}
		}
	}()
}

func createSumBlock(curBlock, newBlock *models.Block) *models.Block {
	//////////////////////
	// Transaction Fees //
	//////////////////////
	transactionFees := curBlock.TransactionFees + newBlock.TransactionFees

	////////////////////////
	// Transaction Amount //
	////////////////////////
	curAmount := new(big.Int)
	newAmount := new(big.Int)

	if len(curBlock.TransactionAmount) < 3 {
		curBlock.TransactionAmount = "0x0"
	}
	if len(newBlock.TransactionAmount) < 3 {
		newBlock.TransactionAmount = "0x0"
	}

	curAmount.SetString(curBlock.TransactionAmount[2:], 16)
	newAmount.SetString(newBlock.TransactionAmount[2:], 16)

	// big.Int
	sumAmount := new(big.Int)
	sumAmount.Add(newAmount, curAmount)

	// Hex string
	transactionAmount := fmt.Sprintf("0x%x", sumAmount)

	//////////////////////////
	// Internal Transaction //
	//////////////////////////
	internalTransactionCount := curBlock.InternalTransactionCount + newBlock.InternalTransactionCount

	//////////////////////////////
	// Failed Transaction Count //
	//////////////////////////////
	failedTransactionCount := curBlock.FailedTransactionCount + newBlock.FailedTransactionCount

	sumBlock := &models.Block{
		Signature:                curBlock.Signature,
		ItemId:                   curBlock.ItemId,
		NextLeader:               curBlock.NextLeader,
		TransactionCount:         curBlock.TransactionCount,
		Type:                     curBlock.Type,
		Version:                  curBlock.Version,
		PeerId:                   curBlock.PeerId,
		Number:                   curBlock.Number,
		MerkleRootHash:           curBlock.MerkleRootHash,
		ItemTimestamp:            curBlock.ItemTimestamp,
		Hash:                     curBlock.Hash,
		ParentHash:               curBlock.ParentHash,
		Timestamp:                curBlock.Timestamp,
		TransactionFees:          transactionFees,
		TransactionAmount:        transactionAmount,
		InternalTransactionCount: internalTransactionCount,
		FailedTransactionCount:   failedTransactionCount,
	}

	return sumBlock
}
