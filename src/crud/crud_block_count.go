package crud

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/models"
)

// BlockCountModel - type for block table model
type BlockCountModel struct {
	db        *gorm.DB
	model     *models.BlockCount
	modelORM  *models.BlockCountORM
	WriteChan chan *models.BlockCount
}

var blockCountModel *BlockCountModel
var blockCountModelOnce sync.Once

// GetBlockModel - create and/or return the blocks table model
func GetBlockCountModel() *BlockCountModel {
	blockCountModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		blockCountModel = &BlockCountModel{
			db:        dbConn,
			model:     &models.BlockCount{},
			WriteChan: make(chan *models.BlockCount, 1),
		}

		err := blockCountModel.Migrate()
		if err != nil {
			zap.S().Fatal("BlockCountModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return blockCountModel
}

// Migrate - migrate blockCounts table
func (m *BlockCountModel) Migrate() error {
	// Only using BlockCountRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert blockCount into table
func (m *BlockCountModel) Insert(blockCount *models.BlockCount) error {

	err := backoff.Retry(func() error {
		query := m.db.Create(blockCount)

		if query.Error != nil && !strings.Contains(query.Error.Error(), "duplicate key value violates unique constraint") {
			zap.S().Warn("POSTGRES Insert Error : ", query.Error.Error())
			return query.Error
		}

		return nil
	}, backoff.NewExponentialBackOff())

	return err
}

// Update - Update blockCount
func (m *BlockCountModel) Update(blockCount *models.BlockCount) error {

	err := backoff.Retry(func() error {
		query := m.db.Model(&models.BlockCount{}).Where("id = ?", blockCount.Id).Update("count", blockCount.Count)

		if query.Error != nil && !strings.Contains(query.Error.Error(), "duplicate key value violates unique constraint") {
			zap.S().Warn("POSTGRES Insert Error : ", query.Error.Error())
			return query.Error
		}

		return nil
	}, backoff.NewExponentialBackOff())

	return err
}

// Select - select from blockCounts table
func (m *BlockCountModel) Select() (models.BlockCount, error) {
	db := m.db

	blockCount := models.BlockCount{}
	db = db.First(&blockCount)

	return blockCount, db.Error
}

// Delete - delete from blockCounts table
func (m *BlockCountModel) Delete(blockCount models.BlockCount) error {
	db := m.db

	db = db.Delete(&blockCount)

	return db.Error
}

// StartBlockCountLoader starts loader
func StartBlockCountLoader() {
	go func() {
		var blockCount *models.BlockCount
		postgresLoaderChan := GetBlockCountModel().WriteChan

		for {
			// Read blockCount
			blockCount = <-postgresLoaderChan

			// Load blockCount to database
			curCount, err := GetBlockCountModel().Select()
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// New entry
				GetBlockCountModel().Insert(blockCount)
			} else if err == nil {
				// Update existing entry
				blockCount.Count = blockCount.Count + curCount.Count
				GetBlockCountModel().Update(blockCount)
			} else {
				// Postgres error
				zap.S().Fatal(err.Error())
			}

			// Check current state
			for {
				// Wait for postgres to set state before processing more messages

				checkCount, err := GetBlockCountModel().Select()
				if err != nil {
					zap.S().Warn("State check error: ", err.Error())
					zap.S().Warn("Waiting 100ms...")
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// check all fields
				if checkCount.Count == blockCount.Count &&
					checkCount.Id == blockCount.Id {
					// Success
					break
				} else {
					// Wait

					zap.S().Warn("Models did not match")
					zap.S().Warn("Waiting 100ms...")
					time.Sleep(100 * time.Millisecond)
					continue
				}
			}
		}
	}()
}
