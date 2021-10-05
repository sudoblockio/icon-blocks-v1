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
	modelValueOf := reflect.ValueOf(*blockTransaction)
	modelTypeOf := reflect.TypeOf(*blockTransaction)
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
