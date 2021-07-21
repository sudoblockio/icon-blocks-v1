package crud_test

import (
	"testing"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"

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
	//logging.StartLoggingInit()

	blockModel = NewBlockModel()
	_ = blockModel.Migrate() // Have to create table before running tests
})

func NewBlockModel() *crud.BlockModel {
	testBlockRawModel := crud.GetBlockModel()
	return testBlockRawModel
}
