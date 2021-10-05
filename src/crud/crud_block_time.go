package crud

import (
	"reflect"
	"strings"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-blocks/models"
)

// BlockTimeModel - type for blockTime table model
type BlockTimeModel struct {
	db            *gorm.DB
	model         *models.BlockTime
	modelORM      *models.BlockTimeORM
	LoaderChannel chan *models.BlockTime
}

var blockTimeModel *BlockTimeModel
var blockTimeModelOnce sync.Once

// GetBlockTimeModel - create and/or return the blockTimes table model
func GetBlockTimeModel() *BlockTimeModel {
	blockTimeModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		blockTimeModel = &BlockTimeModel{
			db:            dbConn,
			model:         &models.BlockTime{},
			LoaderChannel: make(chan *models.BlockTime, 1),
		}

		err := blockTimeModel.Migrate()
		if err != nil {
			zap.S().Fatal("BlockTimeModel: Unable migrate postgres table: ", err.Error())
		}

		StartBlockTimeLoader()
	})

	return blockTimeModel
}

// Migrate - migrate blockTimes table
func (m *BlockTimeModel) Migrate() error {
	// Only using BlockTimeRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert blockTime into table
func (m *BlockTimeModel) Insert(blockTime *models.BlockTime) error {

	err := backoff.Retry(func() error {
		query := m.db.Create(blockTime)
		if query.Error != nil && !strings.Contains(query.Error.Error(), "duplicate key value violates unique constraint") {
			zap.S().Warn("POSTGRES Insert Error : ", query.Error.Error())
			return query.Error
		}

		return nil
	}, backoff.NewExponentialBackOff())

	return err
}

// SelectOne - select from blockTimes table
func (m *BlockTimeModel) SelectOne(
	number uint32,
) (*models.BlockTime, error) {
	db := m.db

	db = db.Order("number desc")

	db = db.Where("number = ?", number)

	blockTime := &models.BlockTime{}
	db = db.First(blockTime)

	return blockTime, db.Error
}

// UpdateOne - select from blockTimes table
func (m *BlockTimeModel) UpdateOne(
	blockTime *models.BlockTime,
) error {
	db := m.db

	db = db.Order("number desc")

	db = db.Where("number = ?", blockTime.Number)

	db = db.Save(blockTime)

	return db.Error
}

func (m *BlockTimeModel) UpsertOne(
	blockTime *models.BlockTime,
) error {
	db := m.db

	// Create map[]interface{} with only non-nil fields
	updateOnConflictValues := map[string]interface{}{}

	// Loop through struct using reflect package
	modelValueOf := reflect.ValueOf(*blockTime)
	modelTypeOf := reflect.TypeOf(*blockTime)
	for i := 0; i < modelValueOf.NumField(); i++ {
		modelField := modelValueOf.Field(i)
		modelType := modelTypeOf.Field(i)

		modelTypeJSONTag := modelType.Tag.Get("json")
		if modelTypeJSONTag != "" {
			// exported field

			// Check if field if filled
			modelFieldKind := modelField.Kind()
			isBlockFieldFilled := true
			switch modelFieldKind {
			case reflect.String:
				v := modelField.Interface().(string)
				if v == "" {
					isBlockFieldFilled = false
				}
			case reflect.Int:
				v := modelField.Interface().(int)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Int8:
				v := modelField.Interface().(int8)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Int16:
				v := modelField.Interface().(int16)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Int32:
				v := modelField.Interface().(int32)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Int64:
				v := modelField.Interface().(int64)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint:
				v := modelField.Interface().(uint)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint8:
				v := modelField.Interface().(uint8)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint16:
				v := modelField.Interface().(uint16)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint32:
				v := modelField.Interface().(uint32)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Uint64:
				v := modelField.Interface().(uint64)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Float32:
				v := modelField.Interface().(float32)
				if v == 0 {
					isBlockFieldFilled = false
				}
			case reflect.Float64:
				v := modelField.Interface().(float64)
				if v == 0 {
					isBlockFieldFilled = false
				}
			}

			if isBlockFieldFilled == true {
				updateOnConflictValues[modelTypeJSONTag] = modelField.Interface()
			}
		}
	}

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "number"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(blockTime)

	return db.Error
}

// StartBlockTimeLoader starts loader
func StartBlockTimeLoader() {
	go func() {

		for {
			// Read blockTime
			newBlockTime := <-GetBlockTimeModel().LoaderChannel

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetBlockTimeModel().UpsertOne(newBlockTime)
			zap.S().Debug("Loader=BlockTime, Number=", newBlockTime.Number, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=BlockTime, Number=", newBlockTime.Number, " - FATAL")
				zap.S().Fatal(err.Error())
			}

			///////////////////////
			// Force enrichments //
			///////////////////////
			err = reloadBlock(newBlockTime.Number)
			if err != nil {
				// Postgress error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
