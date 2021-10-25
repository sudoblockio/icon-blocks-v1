package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/models"
)

// BlockCountIndexModel - type for address table model
type BlockCountIndexModel struct {
	db            *gorm.DB
	model         *models.BlockCountIndex
	modelORM      *models.BlockCountIndexORM
	LoaderChannel chan *models.BlockCountIndex
}

var blockCountIndexModel *BlockCountIndexModel
var blockCountIndexModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetBlockCountIndexModel() *BlockCountIndexModel {
	blockCountIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		blockCountIndexModel = &BlockCountIndexModel{
			db:            dbConn,
			model:         &models.BlockCountIndex{},
			LoaderChannel: make(chan *models.BlockCountIndex, 1),
		}

		err := blockCountIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("BlockCountIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return blockCountIndexModel
}

// Migrate - migrate blockCountIndexs table
func (m *BlockCountIndexModel) Migrate() error {
	// Only using BlockCountIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert blockCountByIndex into table
func (m *BlockCountIndexModel) Insert(blockCountIndex *models.BlockCountIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.BlockCountIndex{})

	db = db.Create(blockCountIndex)

	return db.Error
}
