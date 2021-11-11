package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/models"
)

// KafkaJobModel - type for kafkaJob table model
type KafkaJobModel struct {
	db            *gorm.DB
	model         *models.KafkaJob
	modelORM      *models.KafkaJobORM
	LoaderChannel chan *models.KafkaJob
}

var kafkaJobModel *KafkaJobModel
var kafkaJobModelOnce sync.Once

// GetKafkaJobModel - create and/or return the kafkaJobs table model
func GetKafkaJobModel() *KafkaJobModel {
	kafkaJobModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		kafkaJobModel = &KafkaJobModel{
			db:            dbConn,
			model:         &models.KafkaJob{},
			modelORM:      &models.KafkaJobORM{},
			LoaderChannel: make(chan *models.KafkaJob, 1),
		}

		err := kafkaJobModel.Migrate()
		if err != nil {
			zap.S().Fatal("KafkaJobModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return kafkaJobModel
}

// Migrate - migrate kafkaJobs table
func (m *KafkaJobModel) Migrate() error {
	// Only using KafkaJobRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// SelectMany - select from kafkaJobs table
func (m *KafkaJobModel) SelectMany(
	jobID string,
	workerGroup string,
) (*[]models.KafkaJob, error) {
	db := m.db

	// Job ID
	db = db.Where("job_id = ?", jobID)

	// Worker Group
	db = db.Where("worker_group = ?", workerGroup)

	kafkaJob := &[]models.KafkaJob{}
	db = db.Find(kafkaJob)

	return kafkaJob, db.Error
}
