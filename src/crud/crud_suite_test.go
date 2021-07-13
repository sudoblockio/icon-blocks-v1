package crud_test

import (
	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	blockModel *crud.BlockModel
)

func TestCrud(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Crud Suite")
}

var _ = BeforeSuite(func() {
	config.ReadEnvironment()

	blockModel = NewBlockModel()
	_ = blockModel.Migrate() // Have to create table before running tests
})

func NewBlockModel() *crud.BlockModel {
	dsn := crud.NewDsn("localhost", config.Config.DbPort, config.Config.DbUser, config.Config.DbPassword,
		config.Config.DbTestName, config.Config.DbSslmode, config.Config.DbTimezone)
	postgresConn, _ := crud.NewPostgresConn(dsn)
	testBlockRawModel := crud.NewBlockModel(postgresConn.GetConn())
	return testBlockRawModel
}
