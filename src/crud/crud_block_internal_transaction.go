package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/models"
)

// BlockInternalTransactionModel - type for block table model
type BlockInternalTransactionModel struct {
	db        *gorm.DB
	model     *models.BlockInternalTransaction
	modelORM  *models.BlockInternalTransactionORM
	WriteChan chan *models.BlockInternalTransaction
}

var blockInternalTransactionModel *BlockInternalTransactionModel
var blockInternalTransactionModelOnce sync.Once

// GetBlockModel - create and/or return the blocks table model
func GetBlockInternalTransactionModel() *BlockInternalTransactionModel {
	blockInternalTransactionModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		blockInternalTransactionModel = &BlockInternalTransactionModel{
			db:        dbConn,
			model:     &models.BlockInternalTransaction{},
			WriteChan: make(chan *models.BlockInternalTransaction, 1),
		}

		err := blockInternalTransactionModel.Migrate()
		if err != nil {
			zap.S().Fatal("BlockInternalTransactionModel: Unable migrate postgres table: ", err.Error())
		}

		StartBlockInternalTransactionLoader()
	})

	return blockInternalTransactionModel
}

// Migrate - migrate blockInternalTransactions table
func (m *BlockInternalTransactionModel) Migrate() error {
	// Only using BlockInternalTransactionRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert blockInternalTransaction into table
func (m *BlockInternalTransactionModel) Insert(blockInternalTransaction *models.BlockInternalTransaction) error {
	db := m.db

	// Set table
	db = db.Model(&models.BlockInternalTransaction{})

	db = db.Create(blockInternalTransaction)

	return db.Error
}

// SelectOne - select from blockInternalTransactions table
func (m *BlockInternalTransactionModel) SelectOne(transactionHash string, logIndex uint32) (*models.BlockInternalTransaction, error) {
	db := m.db

	// Set table
	db = db.Model(&models.BlockInternalTransaction{})

	// Transaction hash
	db = db.Where("transaction_hash = ?", transactionHash)

	// Log index
	db = db.Where("log_index = ?", logIndex)

	blockInternalTransaction := &models.BlockInternalTransaction{}
	db = db.First(blockInternalTransaction)

	return blockInternalTransaction, db.Error
}

// SelectMany - select many from blockInternalTransaction table by block number
func (m *BlockInternalTransactionModel) SelectMany(number uint32) (*[]models.BlockInternalTransaction, error) {
	db := m.db

	// Set table
	db = db.Model(&models.BlockInternalTransaction{})

	// Number
	db = db.Where("number = ?", number)

	blockInternalTransactions := &[]models.BlockInternalTransaction{}
	db = db.Find(blockInternalTransactions)

	return blockInternalTransactions, db.Error
}

// UpdateOne - update in blockInternalTransactions table
func (m *BlockInternalTransactionModel) UpdateOne(blockInternalTransaction *models.BlockInternalTransaction) error {
	db := m.db

	// Set table
	db = db.Model(&models.BlockInternalTransaction{})

	// Transaction hash
	db = db.Where("transaction_hash = ?", blockInternalTransaction.TransactionHash)

	// Log index
	db = db.Where("log_index = ?", blockInternalTransaction.LogIndex)

	db = db.First(blockInternalTransaction)

	return db.Error
}

// StartBlockInternalTransactionLoader starts loader
func StartBlockInternalTransactionLoader() {
	go func() {

		for {
			// Read newBlockInternalTransaction
			newBlockInternalTransaction := <-GetBlockInternalTransactionModel().WriteChan

			// Insert
			_, err := GetBlockInternalTransactionModel().SelectOne(
				newBlockInternalTransaction.TransactionHash,
				newBlockInternalTransaction.LogIndex,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Insert
				err = GetBlockInternalTransactionModel().Insert(newBlockInternalTransaction)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=BlockInternalTransaction, Number=", newBlockInternalTransaction.Number, " TransactionHash=", newBlockInternalTransaction.TransactionHash, " LogIndex=", newBlockInternalTransaction.LogIndex, " - Insert")
			} else if err == nil {
				// Update
				err = GetBlockInternalTransactionModel().UpdateOne(newBlockInternalTransaction)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=BlockInternalTransaction, Number=", newBlockInternalTransaction.Number, " TransactionHash=", newBlockInternalTransaction.TransactionHash, " LogIndex=", newBlockInternalTransaction.LogIndex, " - Update")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}

			///////////////////////
			// Force enrichments //
			///////////////////////
			err = reloadBlock(newBlockInternalTransaction.Number)
			if err != nil {
				// Postgress error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
