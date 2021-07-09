package crud_test
//
//import (
//	"github.com/geometry-labs/icon-blocks/crud"
//	"testing"
//
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//)
//
//var (
//	blockModel         *crud.BlockModel
//	blockRawModelMongo *crud.BlockModelMongo
//)
//
//func TestCrud(t *testing.T) {
//	RegisterFailHandler(Fail)
//	RunSpecs(t, "Crud Suite")
//}
//
//var _ = BeforeSuite(func() {
//	blockModel = NewBlockModel()
//	_ = blockModel.Migrate() // Have to create table before running tests
//	blockRawModelMongo = NewBlockModelMongo()
//})
//
//func NewBlockModel() *crud.BlockModel {
//	dsn := crud.NewDsn("localhost", "5432", "postgres", "changeme", "test_db", "disable", "UTC")
//	postgresConn, _ := crud.NewPostgresConn(dsn)
//	testBlockRawModel := crud.NewBlockModel(postgresConn.GetConn())
//	return testBlockRawModel
//}
//
//func NewBlockModelMongo() *crud.BlockModelMongo {
//	mongoConn := crud.NewMongoConn("mongodb://127.0.0.1:27017")
//	blockRawModelMongo := crud.NewBlockRawModelMongo(mongoConn)
//	_ = blockRawModelMongo.SetCollectionHandle("icon_test", "contracts")
//	return blockRawModelMongo
//}
