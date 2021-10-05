package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-blocks/models"
)

// BlockInternalTransactionModel - type for block table model
type BlockInternalTransactionModel struct {
	db            *gorm.DB
	model         *models.BlockInternalTransaction
	modelORM      *models.BlockInternalTransactionORM
	LoaderChannel chan *models.BlockInternalTransaction
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
			db:            dbConn,
			model:         &models.BlockInternalTransaction{},
			LoaderChannel: make(chan *models.BlockInternalTransaction, 1),
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

func (m *BlockInternalTransactionModel) UpsertOne(
	blockInternalTransaction *models.BlockInternalTransaction,
) error {
	db := m.db

	// Create map[]interface{} with only non-nil fields
	updateOnConflictValues := map[string]interface{}{}

	// Loop through struct using reflect package
	blockInternalTransactionValueOf := reflect.ValueOf(*blockInternalTransaction)
	blockInternalTransactionTypeOf := reflect.TypeOf(*blockInternalTransaction)
	for i := 0; i < blockInternalTransactionValueOf.NumField(); i++ {
		blockInternalTransactionField := blockInternalTransactionValueOf.Field(i)
		blockInternalTransactionType := blockInternalTransactionTypeOf.Field(i)

		blockInternalTransactionTypeJSONTag := blockInternalTransactionType.Tag.Get("json")
		if blockInternalTransactionTypeJSONTag != "" {
			// exported field

			// Check if field if filled
			blockInternalTransactionFieldKind := blockInternalTransactionField.Kind()
			isBlockFieldFilled := true
			switch blockInternalTransactionFieldKind {
			case reflect.String:
				v := blockInternalTransactionField.Interface().(string)
				if v == "" {
					isBlockFieldFilled = false
				}
			case reflect.Int:
				v := blockInternalTransactionField.Interface().(int)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Int8:
				v := blockInternalTransactionField.Interface().(int8)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Int16:
				v := blockInternalTransactionField.Interface().(int16)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Int32:
				v := blockInternalTransactionField.Interface().(int32)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Int64:
				v := blockInternalTransactionField.Interface().(int64)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint:
				v := blockInternalTransactionField.Interface().(uint)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint8:
				v := blockInternalTransactionField.Interface().(uint8)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint16:
				v := blockInternalTransactionField.Interface().(uint16)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint32:
				v := blockInternalTransactionField.Interface().(uint32)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint64:
				v := blockInternalTransactionField.Interface().(uint64)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Float32:
				v := blockInternalTransactionField.Interface().(float32)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Float64:
				v := blockInternalTransactionField.Interface().(float64)
				if v == 0 {
					isBlockFieldFilled = false
				}
			}

			if isBlockFieldFilled == true {
				updateOnConflictValues[blockInternalTransactionTypeJSONTag] = blockInternalTransactionField.Interface()
			}
		}
	}

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}, {Name: "log_index"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(blockInternalTransaction)

	return db.Error
}

// StartBlockInternalTransactionLoader starts loader
func StartBlockInternalTransactionLoader() {
	go func() {

		for {
			// Read newBlockInternalTransaction
			newBlockInternalTransaction := <-GetBlockInternalTransactionModel().LoaderChannel

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetBlockInternalTransactionModel().UpsertOne(newBlockInternalTransaction)
			zap.S().Debug("Loader=BlockInternalTransaction, Number=", newBlockInternalTransaction.Number, " TransactionHash=", newBlockInternalTransaction.TransactionHash, " LogIndex=", newBlockInternalTransaction.LogIndex, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=BlockInternalTransaction, Number=", newBlockInternalTransaction.Number, " TransactionHash=", newBlockInternalTransaction.TransactionHash, " LogIndex=", newBlockInternalTransaction.LogIndex, " - FATAL")
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
