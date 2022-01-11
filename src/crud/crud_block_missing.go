package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-blocks/models"
)

// BlockMissingModel - type for address table model
type BlockMissingModel struct {
	db            *gorm.DB
	model         *models.BlockMissing
	modelORM      *models.BlockMissingORM
	LoaderChannel chan *models.BlockMissing
}

var blockMissingModel *BlockMissingModel
var blockMissingModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetBlockMissingModel() *BlockMissingModel {
	blockMissingModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		blockMissingModel = &BlockMissingModel{
			db:            dbConn,
			model:         &models.BlockMissing{},
			LoaderChannel: make(chan *models.BlockMissing, 1),
		}

		err := blockMissingModel.Migrate()
		if err != nil {
			zap.S().Fatal("BlockMissingModel: Unable migrate postgres table: ", err.Error())
		}

		StartBlockMissingLoader()
	})

	return blockMissingModel
}

// Migrate - migrate blockMissings table
func (m *BlockMissingModel) Migrate() error {
	// Only using BlockMissingRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

func (m *BlockMissingModel) UpsertOne(
	blockMissing *models.BlockMissing,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*blockMissing),
		reflect.TypeOf(*blockMissing),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "number"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(blockMissing)

	return db.Error
}

func StartBlockMissingLoader() {
	go func() {

		for {
			// Read block
			newBlockMissing := <-GetBlockMissingModel().LoaderChannel

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetBlockMissingModel().UpsertOne(newBlockMissing)
			zap.S().Debug("Loader=BlockMissing, Number=", newBlockMissing.Number, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=BlockMissing, Number=", newBlockMissing.Number, " - Error: ", err.Error())
			}
		}
	}()
}
