package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-blocks/models"
)

// BlockFailedTransactionModel - type for block table model
type BlockFailedTransactionModel struct {
	db            *gorm.DB
	model         *models.BlockFailedTransaction
	modelORM      *models.BlockFailedTransactionORM
	LoaderChannel chan *models.BlockFailedTransaction
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
			db:            dbConn,
			model:         &models.BlockFailedTransaction{},
			LoaderChannel: make(chan *models.BlockFailedTransaction, 1),
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

	// Number
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

func (m *BlockFailedTransactionModel) UpsertOne(
	blockFailedTransaction *models.BlockFailedTransaction,
) error {
	db := m.db

	// Create map[]interface{} with only non-nil fields
	updateOnConflictValues := map[string]interface{}{}

	// Loop through struct using reflect package
	modelValueOf := reflect.ValueOf(*blockFailedTransaction)
	modelTypeOf := reflect.TypeOf(*blockFailedTransaction)
	for i := 0; i < modelValueOf.NumField(); i++ {
		modelField := modelValueOf.Field(i)
		modelType := modelTypeOf.Field(i)

		modelTypeJSONTag := modelType.Tag.Get("json")
		if modelTypeJSONTag != "" {
			// exported field

			// Check if field if filled
			modelFieldKind := modelField.Kind()
			isFieldFilled := true
			switch modelFieldKind {
			case reflect.String:
				v := modelField.Interface().(string)
				if v == "" {
					isFieldFilled = false
				}
			case reflect.Int:
				v := modelField.Interface().(int)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Int8:
				v := modelField.Interface().(int8)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Int16:
				v := modelField.Interface().(int16)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Int32:
				v := modelField.Interface().(int32)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Int64:
				v := modelField.Interface().(int64)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Uint:
				v := modelField.Interface().(uint)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Uint8:
				v := modelField.Interface().(uint8)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Uint16:
				v := modelField.Interface().(uint16)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Uint32:
				v := modelField.Interface().(uint32)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Uint64:
				v := modelField.Interface().(uint64)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Float32:
				v := modelField.Interface().(float32)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Float64:
				v := modelField.Interface().(float64)
				if v == 0 {
					isFieldFilled = false
				}
			}

			if isFieldFilled == true {
				updateOnConflictValues[modelTypeJSONTag] = modelField.Interface()
			}
		}
	}

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(blockFailedTransaction)

	return db.Error
}

// StartBlockFailedTransactionLoader starts loader
func StartBlockFailedTransactionLoader() {
	go func() {

		for {
			// Read newBlockFailedTransaction
			newBlockFailedTransaction := <-GetBlockFailedTransactionModel().LoaderChannel

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetBlockFailedTransactionModel().UpsertOne(newBlockFailedTransaction)
			zap.S().Debug("Loader=BlockFailedTransaction, TransactionHash=", newBlockFailedTransaction.TransactionHash, " - Upserted")
			if err != nil {
				// Error
				zap.S().Info("Loader=BlockFailedTransaction, TransactionHash=", newBlockFailedTransaction.TransactionHash, " - FATAL")
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
