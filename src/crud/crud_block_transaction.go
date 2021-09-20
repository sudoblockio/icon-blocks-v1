package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/models"
)

// BlockTransactionModel - type for block table model
type BlockTransactionModel struct {
	db            *gorm.DB
	model         *models.BlockTransaction
	modelORM      *models.BlockTransactionORM
	LoaderChannel chan *models.BlockTransaction
}

var blockTransactionModel *BlockTransactionModel
var blockTransactionModelOnce sync.Once

// GetBlockModel - create and/or return the blocks table model
func GetBlockTransactionModel() *BlockTransactionModel {
	blockTransactionModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		blockTransactionModel = &BlockTransactionModel{
			db:            dbConn,
			model:         &models.BlockTransaction{},
			LoaderChannel: make(chan *models.BlockTransaction, 1),
		}

		err := blockTransactionModel.Migrate()
		if err != nil {
			zap.S().Fatal("BlockTransactionModel: Unable migrate postgres table: ", err.Error())
		}

		StartBlockTransactionLoader()
	})

	return blockTransactionModel
}

// Migrate - migrate blockTransactions table
func (m *BlockTransactionModel) Migrate() error {
	// Only using BlockTransactionRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert blockTransaction into table
func (m *BlockTransactionModel) Insert(blockTransaction *models.BlockTransaction) error {
	db := m.db

	// Set table
	db = db.Model(&models.BlockTransaction{})

	db = db.Create(blockTransaction)

	return db.Error
}

// SelectOne - select from blockTransactions table
func (m *BlockTransactionModel) SelectOne(transactionHash string) (*models.BlockTransaction, error) {
	db := m.db

	// Set table
	db = db.Model(&models.BlockTransaction{})

	// Transaction hash
	db = db.Where("transaction_hash = ?", transactionHash)

	blockTransaction := &models.BlockTransaction{}
	db = db.First(blockTransaction)

	return blockTransaction, db.Error
}

// SelectMany - select many from blockTransactions table by block number
func (m *BlockTransactionModel) SelectMany(number uint32) (*[]models.BlockTransaction, error) {
	db := m.db

	// Set table
	db = db.Model(&models.BlockTransaction{})

	// Number
	db = db.Where("number = ?", number)

	blockTransactions := &[]models.BlockTransaction{}
	db = db.Find(blockTransactions)

	return blockTransactions, db.Error
}

// UpdateOne - update in blockTransactions table
func (m *BlockTransactionModel) UpdateOne(blockTransaction *models.BlockTransaction) error {
	db := m.db

	// Set table
	db = db.Model(&models.BlockTransaction{})

	// Transaction hash
	db = db.Where("transaction_hash = ?", blockTransaction.TransactionHash)

	db = db.First(blockTransaction)

	return db.Error
}

// StartBlockTransactionLoader starts loader
func StartBlockTransactionLoader() {
	go func() {

		for {
			// Read newBlockTransaction
			newBlockTransaction := <-GetBlockTransactionModel().LoaderChannel

			// Insert
			_, err := GetBlockTransactionModel().SelectOne(
				newBlockTransaction.TransactionHash,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Insert
				err = GetBlockTransactionModel().Insert(newBlockTransaction)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=BlockTransaction, Number=", newBlockTransaction.Number, " TransactionHash=", newBlockTransaction.TransactionHash, " - Insert")
			} else if err == nil {
				// Update
				err = GetBlockTransactionModel().UpdateOne(newBlockTransaction)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=BlockTransaction, Number=", newBlockTransaction.Number, " TransactionHash=", newBlockTransaction.TransactionHash, " - Update")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}

			///////////////////////
			// Force enrichments //
			///////////////////////
			err = reloadBlock(newBlockTransaction.Number)
			if err != nil {
				// Postgress error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
