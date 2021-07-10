package crud_test

import (
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
	blockModel = NewBlockModel()
	_ = blockModel.Migrate() // Have to create table before running tests
})

func NewBlockModel() *crud.BlockModel {
	dsn := crud.NewDsn("localhost", "5432", "postgres", "changeme", "test_db", "disable", "UTC")
	postgresConn, _ := crud.NewPostgresConn(dsn)
	testBlockRawModel := crud.NewBlockModel(postgresConn.GetConn())
	return testBlockRawModel
}
