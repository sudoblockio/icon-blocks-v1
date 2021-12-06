package crud

import (
	"errors"
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-blocks/models"
	"github.com/geometry-labs/icon-blocks/redis"
)

// BlockCountModel - type for address table model
type BlockCountModel struct {
	db            *gorm.DB
	model         *models.BlockCount
	modelORM      *models.BlockCountORM
	LoaderChannel chan *models.BlockCount
}

var blockCountModel *BlockCountModel
var blockCountModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetBlockCountModel() *BlockCountModel {
	blockCountModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		blockCountModel = &BlockCountModel{
			db:            dbConn,
			model:         &models.BlockCount{},
			LoaderChannel: make(chan *models.BlockCount, 1),
		}

		err := blockCountModel.Migrate()
		if err != nil {
			zap.S().Fatal("BlockCountModel: Unable migrate postgres table: ", err.Error())
		}

		StartBlockCountLoader()
	})

	return blockCountModel
}

// Migrate - migrate blockCounts table
func (m *BlockCountModel) Migrate() error {
	// Only using BlockCountRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Select - select from blockCounts table
func (m *BlockCountModel) SelectOne(_type string) (*models.BlockCount, error) {
	db := m.db

	// Set table
	db = db.Model(&models.BlockCount{})

	// Address
	db = db.Where("type = ?", _type)

	blockCount := &models.BlockCount{}
	db = db.First(blockCount)

	return blockCount, db.Error
}

// Select - select from blockCounts table
func (m *BlockCountModel) SelectCount(_type string) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.BlockCount{})

	// Address
	db = db.Where("type = ?", _type)

	blockCount := &models.BlockCount{}
	db = db.First(blockCount)

	count := uint64(0)
	if blockCount != nil {
		// NOTE just use block number for count
		count = uint64(blockCount.Number)
	}

	return count, db.Error
}

func (m *BlockCountModel) UpsertOne(
	blockCount *models.BlockCount,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*blockCount),
		reflect.TypeOf(*blockCount),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "type"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(blockCount)

	return db.Error
}

// StartBlockCountLoader starts loader
func StartBlockCountLoader() {
	go func() {
		postgresLoaderChan := GetBlockCountModel().LoaderChannel

		for {
			// Read block
			newBlockCount := <-postgresLoaderChan

			//////////////////////////
			// Get count from redis //
			//////////////////////////
			countKey := "icon_blocks_block_count_" + newBlockCount.Type

			count, err := redis.GetRedisClient().GetCount(countKey)
			if err != nil {
				zap.S().Fatal(
					"Loader=Block,",
					"Number=", newBlockCount.Number,
					" Type=", newBlockCount.Type,
					" - Error: ", err.Error())
			}

			// No count set yet
			// Get from database
			if count == -1 {
				curBlockCount, err := GetBlockCountModel().SelectOne(newBlockCount.Type)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					count = 0
				} else if err != nil {
					zap.S().Fatal(
						"Loader=Block,",
						"Number=", newBlockCount.Number,
						" Type=", newBlockCount.Type,
						" - Error: ", err.Error())
				} else {
					count = int64(curBlockCount.Count)
				}

				// Set count
				err = redis.GetRedisClient().SetCount(countKey, int64(count))
				if err != nil {
					// Redis error
					zap.S().Fatal(
						"Loader=Block,",
						"Number=", newBlockCount.Number,
						" Type=", newBlockCount.Type,
						" - Error: ", err.Error())
				}
			}

			//////////////////////
			// Load to postgres //
			//////////////////////

			// Add block to indexed
			if newBlockCount.Type == "block" {
				newBlockCountIndex := &models.BlockCountIndex{
					Number: newBlockCount.Number,
				}
				err = GetBlockCountIndexModel().Insert(newBlockCountIndex)
				if err != nil {
					// Record already exists, continue
					continue
				}
			}

			// Increment records
			count, err = redis.GetRedisClient().IncCount(countKey)
			if err != nil {
				// Redis error
				zap.S().Fatal(
					"Loader=Block,",
					"Number=", newBlockCount.Number,
					" Type=", newBlockCount.Type,
					" - Error: ", err.Error())
			}
			newBlockCount.Count = uint64(count)

			err = GetBlockCountModel().UpsertOne(newBlockCount)
			zap.S().Debug(
				"Loader=Block,",
				"Number=", newBlockCount.Number,
				" Type=", newBlockCount.Type,
				" - Upsert")
			if err != nil {
				// Postgres error
				zap.S().Fatal(
					"Loader=Block,",
					"Number=", newBlockCount.Number,
					" Type=", newBlockCount.Type,
					" - Error: ", err.Error())
			}
		}
	}()
}
