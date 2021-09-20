package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/models"
)

// BlockCountModel - type for block table model
type BlockCountModel struct {
	db            *gorm.DB
	model         *models.BlockCount
	modelORM      *models.BlockCountORM
	LoaderChannel chan *models.BlockCount
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

// Insert - Insert blockCount into table
func (m *BlockCountModel) Insert(blockCount *models.BlockCount) error {
	db := m.db

	// Set table
	db = db.Model(&models.BlockCount{})

	db = db.Create(blockCount)

	return db.Error
}

// Select - select from blockCounts table
func (m *BlockCountModel) SelectOne(number uint32) (*models.BlockCount, error) {
	db := m.db

	// Set table
	db = db.Model(&models.BlockCount{})

	// Number
	db = db.Where("number = ?", number)

	blockCount := &models.BlockCount{}
	db = db.First(blockCount)

	return blockCount, db.Error
}

func (m *BlockCountModel) SelectLargestCount() (uint32, error) {

	db := m.db
	//computeCount := false

	// Set table
	db = db.Model(&models.BlockCount{})

	// Get max id
	count := uint32(0)
	row := db.Select("max(id)").Row()
	row.Scan(&count)

	return count, db.Error
}

// StartBlockCountLoader starts loader
func StartBlockCountLoader() {
	go func() {

		for {
			// Read newBlockCount
			newBlockCount := <-GetBlockCountModel().LoaderChannel

			// Insert
			_, err := GetBlockCountModel().SelectOne(
				newBlockCount.Number,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Insert
				err = GetBlockCountModel().Insert(newBlockCount)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=BlockCount, Number=", newBlockCount.Number, " - Insert")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
