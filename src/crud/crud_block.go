package crud

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/geometry-labs/icon-blocks/models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strings"
)

type BlockModel struct {
	db        *gorm.DB
	model     *models.Block
	modelORM  *models.BlockORM
  writeChan chan *models.Block
}

var blockModel *BlockModel

func GetBlockModel() *BlockModel {
  if blockModel == nil {
		blockModel = &BlockModel{
			db:        GetPostgresConn().conn,
			model:     &models.Block{},
			writeChan: make(chan *models.Block, 1),
		}

		err := blockModel.Migrate()
		if err != nil {
			zap.S().Error("BlockModel: Unable create postgres table: Blocks")
		}
  }

	return blockModel
}

func (m *BlockModel) Migrate() error {
	// Only using BlockRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

func (m *BlockModel) Create(block *models.Block) error {

	err := backoff.Retry(func() error {
		query := m.db.Create(block)
		if query.Error != nil && !strings.Contains(query.Error.Error(), "duplicate key value violates unique constraint") {
			zap.S().Warn("POSTGRES RetryCreate Error : ", query.Error.Error())
		  return query.Error
		}

		return nil
	}, backoff.NewExponentialBackOff())

	return err
}

func (m *BlockModel) Select(
  limit         int,
  skip          int,
  number        uint32,
  start_number  uint32,
  end_number    uint32,
  hash          string,
  created_by    string,
) (*[]models.Block) {
  db := m.db

  // Limit is required and defaulted to 1
  db = db.Limit(limit)

  // Skip
  if skip != 0 {
    db = db.Offset(skip)
  }

  // Height
  if number != 0 {
    db = db.Where("number = ?", number)
  }

  // Start number and end number
  if start_number != 0 && end_number != 0 {
    db = db.Where("number BETWEEN ? AND ?", start_number, end_number)
  } else if start_number != 0 {
    db = db.Where("number > ?", start_number)
  } else if end_number != 0 {
    db = db.Where("number < ?", end_number)
  }

  // Hash
  if hash != "" {
    db = db.Where("hash = ?", hash)
  }

  // Created By
  if created_by != "" {
    db = db.Where("created_by = ?", created_by)
  }

  blocks := &[]models.Block{}
  db.Find(blocks)

  return blocks
}

func (m *BlockModel) FindOne(conds ...interface{}) (*models.Block, *gorm.DB) {
	block := &models.Block{}
	tx := m.db.Find(block, conds...)
	return block, tx
}

func (m *BlockModel) FindAll(conds ...interface{}) (*[]models.Block, *gorm.DB) {
	blocks := &[]models.Block{}
	tx := m.db.Scopes().Find(blocks, conds...)
	return blocks, tx
}
