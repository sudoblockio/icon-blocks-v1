package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/models"
)

// BlockFailedTransactionModel - type for block table model
type BlockFailedTransactionModel struct {
	db        *gorm.DB
	model     *models.BlockFailedTransaction
	modelORM  *models.BlockFailedTransactionORM
	WriteChan chan *models.BlockFailedTransaction
}

var blockFailedTransactionModel *BlockFailedTransactionModel
var blockFailedTransactionModelOnce sync.Once

// GetBlockModel - create and/or return the blocks table model
func GetBlockFailedTransactionModel() *BlockFailedTransactionModel {
	blockFailedTransactionModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		blockFailedTransactionModel = &BlockFailedTransactionModel{
			db:        dbConn,
			model:     &models.BlockFailedTransaction{},
			WriteChan: make(chan *models.BlockFailedTransaction, 1),
		}

		err := blockFailedTransactionModel.Migrate()
		if err != nil {
			zap.S().Fatal("BlockFailedTransactionModel: Unable migrate postgres table: ", err.Error())
		}

		StartBlockFailedTransactionLoader()
	})

	return blockFailedTransactionModel
}

// Migrate - migrate blockFailedTransactions table
func (m *BlockFailedTransactionModel) Migrate() error {
	// Only using BlockFailedTransactionRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert blockFailedTransaction into table
func (m *BlockFailedTransactionModel) Insert(blockFailedTransaction *models.BlockFailedTransaction) error {
	db := m.db

	// Set table
	db = db.Model(&models.BlockFailedTransaction{})

	db = db.Create(blockFailedTransaction)

	return db.Error
}

// SelectOne - select from blockFailedTransactions table
func (m *BlockFailedTransactionModel) SelectOne(transactionHash string) (*models.BlockFailedTransaction, error) {
	db := m.db

	// Set table
	db = db.Model(&models.BlockFailedTransaction{})

	// Transaction hash
	db = db.Where("transaction_hash = ?", transactionHash)

	blockFailedTransaction := &models.BlockFailedTransaction{}
	db = db.First(blockFailedTransaction)

	return blockFailedTransaction, db.Error
}

// SelectMany - select many from blockFailedTransactions table by block number
func (m *BlockFailedTransactionModel) SelectMany(number uint32) (*[]models.BlockFailedTransaction, error) {
	db := m.db

	// Set table
	db = db.Model(&models.BlockFailedTransaction{})

	// Transaction hash
	db = db.Where("number = ?", number)

	blockFailedTransactions := &[]models.BlockFailedTransaction{}
	db = db.Find(blockFailedTransactions)

	return blockFailedTransactions, db.Error
}

// UpdateOne - update in blockFailedTransactions table
func (m *BlockFailedTransactionModel) UpdateOne(blockFailedTransaction *models.BlockFailedTransaction) error {
	db := m.db

	// Set table
	db = db.Model(&models.BlockFailedTransaction{})

	// Transaction hash
	db = db.Where("transaction_hash = ?", blockFailedTransaction.TransactionHash)

	db = db.First(blockFailedTransaction)

	return db.Error
}

// StartBlockFailedTransactionLoader starts loader
func StartBlockFailedTransactionLoader() {
	go func() {

		for {
			// Read newBlockFailedTransaction
			newBlockFailedTransaction := <-GetBlockFailedTransactionModel().WriteChan

			// Insert
			_, err := GetBlockFailedTransactionModel().SelectOne(
				newBlockFailedTransaction.TransactionHash,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Insert
				err = GetBlockFailedTransactionModel().Insert(newBlockFailedTransaction)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=BlockFailedTransaction, Number=", newBlockFailedTransaction.Number, " TransactionHash=", newBlockFailedTransaction.TransactionHash, " - Insert")
			} else if err == nil {
				// Update
				err = GetBlockFailedTransactionModel().UpdateOne(newBlockFailedTransaction)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=BlockFailedTransaction, Number=", newBlockFailedTransaction.Number, " TransactionHash=", newBlockFailedTransaction.TransactionHash, " - Update")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}

			///////////////////////
			// Force enrichments //
			///////////////////////
			err = reloadBlock(newBlockFailedTransaction.Number)
			if err != nil {
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
