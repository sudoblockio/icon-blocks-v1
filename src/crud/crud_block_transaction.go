package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

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

func (m *BlockTransactionModel) UpsertOne(
	blockTransaction *models.BlockTransaction,
) error {
	db := m.db

	// Create map[]interface{} with only non-nil fields
	updateOnConflictValues := map[string]interface{}{}

	// Loop through struct using reflect package
	blockTransactionValueOf := reflect.ValueOf(*blockTransaction)
	blockTransactionTypeOf := reflect.TypeOf(*blockTransaction)
	for i := 0; i < blockTransactionValueOf.NumField(); i++ {
		blockTransactionField := blockTransactionValueOf.Field(i)
		blockTransactionType := blockTransactionTypeOf.Field(i)

		blockTransactionTypeJSONTag := blockTransactionType.Tag.Get("json")
		if blockTransactionTypeJSONTag != "" {
			// exported field

			// Check if field if filled
			blockTransactionFieldKind := blockTransactionField.Kind()
			isBlockFieldFilled := true
			switch blockTransactionFieldKind {
			case reflect.String:
				v := blockTransactionField.Interface().(string)
				if v == "" {
					isBlockFieldFilled = false
				}
			case reflect.Int:
				v := blockTransactionField.Interface().(int)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Int8:
				v := blockTransactionField.Interface().(int8)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Int16:
				v := blockTransactionField.Interface().(int16)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Int32:
				v := blockTransactionField.Interface().(int32)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Int64:
				v := blockTransactionField.Interface().(int64)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint:
				v := blockTransactionField.Interface().(uint)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint8:
				v := blockTransactionField.Interface().(uint8)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint16:
				v := blockTransactionField.Interface().(uint16)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint32:
				v := blockTransactionField.Interface().(uint32)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint64:
				v := blockTransactionField.Interface().(uint64)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Float32:
				v := blockTransactionField.Interface().(float32)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Float64:
				v := blockTransactionField.Interface().(float64)
				if v == 0 {
					isBlockFieldFilled = false
				}
			}

			if isBlockFieldFilled == true {
				updateOnConflictValues[blockTransactionTypeJSONTag] = blockTransactionField.Interface()
			}
		}
	}

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(blockTransaction)

	return db.Error
}

// StartBlockTransactionLoader starts loader
func StartBlockTransactionLoader() {
	go func() {

		for {
			// Read newBlockTransaction
			newBlockTransaction := <-GetBlockTransactionModel().LoaderChannel

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetBlockTransactionModel().UpsertOne(newBlockTransaction)
			zap.S().Debug("Loader=BlockTransaction, Number=", newBlockTransaction.Number, " TransactionHash=", newBlockTransaction.TransactionHash, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=BlockTransaction, Number=", newBlockTransaction.Number, " TransactionHash=", newBlockTransaction.TransactionHash, " - FATAL")
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
