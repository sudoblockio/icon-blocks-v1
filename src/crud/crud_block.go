package crud

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/geometry-labs/icon-blocks/models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strings"
	"sync"
)

type BlockModel struct {
	db        *gorm.DB
	model     *models.Block
	modelORM  *models.BlockORM
	writeChan chan *models.Block
}

var blockModelInstance *BlockModel
var blockModelOnce sync.Once

func GetBlockModel() *BlockModel {
	blockModelOnce.Do(func() {
		blockModelInstance = &BlockModel{
			db:        GetPostgresConn().conn,
			model:     &models.Block{},
			writeChan: make(chan *models.Block, 1),
		}

		err := blockModelInstance.Migrate()
		if err != nil {
			zap.S().Error("BlockModel: Unable create postgres table: Blocks")
		}
	})
	return blockModelInstance
}

func NewBlockModel(conn *gorm.DB) *BlockModel { // Only for testing
	blockModelInstance = &BlockModel{
		db:        conn,
		model:     &models.Block{},
		writeChan: make(chan *models.Block, 1),
	}
	return blockModelInstance
}

func (m *BlockModel) GetDB() *gorm.DB {
	return m.db
}

func (m *BlockModel) GetModel() *models.Block {
	return m.model
}

func (m *BlockModel) GetWriteChan() chan *models.Block {
	return m.writeChan
}

func (m *BlockModel) Migrate() error {
	// Only using BlockRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

func (m *BlockModel) create(block *models.Block) (*gorm.DB, error) {
	tx := m.db.Create(block)
	return tx, tx.Error
}

func (m *BlockModel) RetryCreate(block *models.Block) (*gorm.DB, error) {
	var transaction *gorm.DB
	operation := func() error {
		tx, err := m.create(block)
		if err != nil && !strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
			zap.S().Info("POSTGRES RetryCreate Error : ", err.Error())
		} else {
			transaction = tx
			return nil
		}
		return err
	}
	neb := backoff.NewExponentialBackOff()
	err := backoff.Retry(operation, neb)
	return transaction, err
}

// TODO: Delete this
func (m *BlockModel) Update(oldBlock *models.Block, newBlock *models.Block, whereClause ...interface{}) *gorm.DB {
	tx := m.db.Model(oldBlock).Where(whereClause[0], whereClause[1:]).Updates(newBlock)
	return tx
}

// TODO: Delete this
// TODO: Mv to crud_integration_test
func (m *BlockModel) Delete(conds ...interface{}) *gorm.DB {
	tx := m.db.Delete(m.model, conds...)
	return tx
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
