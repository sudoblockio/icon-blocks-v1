package crud

import (
	"encoding/json"
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/models"
	"github.com/geometry-labs/icon-blocks/redis"
)

// BlockWebsocketIndexModel - type for blockWebsocketIndex table model
type BlockWebsocketIndexModel struct {
	db            *gorm.DB
	model         *models.BlockWebsocketIndex
	modelORM      *models.BlockWebsocketIndexORM
	LoaderChannel chan *models.BlockWebsocket // Write BlockWebsocket to create a BlockWebsocketIndex
}

var blockWebsocketIndexModel *BlockWebsocketIndexModel
var blockWebsocketIndexModelOnce sync.Once

// GetBlockWebsocketIndexModel - create and/or return the blockWebsocketIndexs table model
func GetBlockWebsocketIndexModel() *BlockWebsocketIndexModel {
	blockWebsocketIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		blockWebsocketIndexModel = &BlockWebsocketIndexModel{
			db:            dbConn,
			model:         &models.BlockWebsocketIndex{},
			LoaderChannel: make(chan *models.BlockWebsocket, 1),
		}

		err := blockWebsocketIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("BlockWebsocketIndexModel: Unable migrate postgres table: ", err.Error())
		}

		StartBlockWebsocketIndexLoader()
	})

	return blockWebsocketIndexModel
}

// Migrate - migrate blockWebsocketIndexs table
func (m *BlockWebsocketIndexModel) Migrate() error {
	// Only using BlockWebsocketIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert blockWebsocketIndex into table
func (m *BlockWebsocketIndexModel) Insert(blockWebsocketIndex *models.BlockWebsocketIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.BlockWebsocketIndex{})

	db = db.Create(blockWebsocketIndex)

	return db.Error
}

func (m *BlockWebsocketIndexModel) SelectOne(
	number uint32,
) (*models.BlockWebsocketIndex, error) {
	db := m.db

	// Set table
	db = db.Model(&models.BlockWebsocketIndex{})

	db = db.Where("number = ?", number)

	blockWebsocketIndex := &models.BlockWebsocketIndex{}
	db = db.First(blockWebsocketIndex)

	return blockWebsocketIndex, db.Error
}

// StartBlockWebsocketIndexLoader starts loader
func StartBlockWebsocketIndexLoader() {
	go func() {

		for {
			// Read block
			newBlockWebsocket := <-GetBlockWebsocketIndexModel().LoaderChannel

			// BlockWebsocket -> BlockWebsocketIndex
			newBlockWebsocketIndex := &models.BlockWebsocketIndex{
				Number: newBlockWebsocket.Number,
			}

			// Insert
			_, err := GetBlockWebsocketIndexModel().SelectOne(newBlockWebsocketIndex.Number)
			if errors.Is(err, gorm.ErrRecordNotFound) {

				// Insert
				err = GetBlockWebsocketIndexModel().Insert(newBlockWebsocketIndex)
				if err != nil {
					zap.S().Warn("Loader=Block, Number=", newBlockWebsocket.Number, " - Error: ", err.Error())
				}

				// Publish to redis
				newBlockWebsocketJSON, _ := json.Marshal(newBlockWebsocket)
				redis.GetRedisClient().Publish(newBlockWebsocketJSON)
			} else if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=Block, Number=", newBlockWebsocket.Number, " - Error: ", err.Error())
			}
		}
	}()
}
