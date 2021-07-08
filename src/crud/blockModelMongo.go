package crud

import (
	"github.com/geometry-labs/icon-blocks/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"sync"
)

type BlockModelMongo struct {
	mongoConn *MongoConn
	model     *models.Block
	//databaseHandle   *mongo.Database
	collectionHandle *mongo.Collection
}

type KeyValue struct {
	Key   string
	Value interface{}
}

var blockRawModelMongoInstance *BlockModelMongo
var blockRawModelMongoOnce sync.Once

func GetBlockRawModelMongo() *BlockModelMongo {
	blockRawModelMongoOnce.Do(func() {
		blockRawModelMongoInstance = &BlockModelMongo{
			mongoConn: GetMongoConn(),
			model:     &models.Block{},
		}
	})
	return blockRawModelMongoInstance
}

func NewBlockRawModelMongo(conn *MongoConn) *BlockModelMongo {
	blockRawModelMongoInstance := &BlockModelMongo{
		mongoConn: conn,
		model:     &models.Block{},
	}
	return blockRawModelMongoInstance
}

//func (b *BlockModelMongo) SetCollectionHandle(collection *mongo.Collection) {
//	b.collectionHandle = collection
//}

func (b *BlockModelMongo) GetMongoConn() *MongoConn {
	return b.mongoConn
}

func (b *BlockModelMongo) GetModel() *models.Block {
	return b.model
}

func (b *BlockModelMongo) SetCollectionHandle(database string, collection string) *mongo.Collection {
	b.collectionHandle = b.mongoConn.DatabaseHandle(database).Collection(collection)
	return b.collectionHandle
}

func (b *BlockModelMongo) GetCollectionHandle() *mongo.Collection {
	return b.collectionHandle
}

func (b *BlockModelMongo) InsertOne(block *models.Block) (*mongo.InsertOneResult, error) {
	one, err := b.collectionHandle.InsertOne(b.mongoConn.ctx, block)
	return one, err
}

func (b *BlockModelMongo) DeleteMany(kv *KeyValue) (*mongo.DeleteResult, error) {
	delR, err := b.collectionHandle.DeleteMany(b.mongoConn.ctx, bson.D{{kv.Key, kv.Value}})
	return delR, err
}

func (b *BlockModelMongo) find(kv *KeyValue) (*mongo.Cursor, error) {
	cursor, err := b.collectionHandle.Find(b.mongoConn.ctx, bson.D{{kv.Key, kv.Value}})
	return cursor, err
}

func (b *BlockModelMongo) FindAll(kv *KeyValue) []bson.M {
	cursor, err := b.find(kv)
	if err != nil {
		zap.S().Info("Exception in getting a curser to a find in mongodb: ", err)
	}
	var results []bson.M
	if err = cursor.All(b.mongoConn.ctx, &results); err != nil {
		zap.S().Info("Exception in find all: ", err)
	}
	return results

}
