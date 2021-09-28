package crud

import (
	"errors"
	"strings"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"

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

// StartBlockTimeLoader starts loader
func StartBlockTimeLoader() {
	go func() {

		for {
			// Read blockTime
			newBlockTime := <-GetBlockTimeModel().LoaderChannel

			//////////////////////
			// Load to postgres //
			//////////////////////
			_, err := GetBlockTimeModel().SelectOne(newBlockTime.Number)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Insert
				GetBlockTimeModel().Insert(newBlockTime)
			} else if err == nil {
				// Update
				GetBlockTimeModel().UpdateOne(newBlockTime)
				zap.S().Debug("Loader=BlockTime, Number=", newBlockTime.Number, " - Updated")
			} else {
				// Postgres error
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
