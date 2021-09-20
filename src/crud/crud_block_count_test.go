//+build unit

package crud

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/logging"
	"github.com/geometry-labs/icon-blocks/models"
)

func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()

	// Set up logging
	logging.Init()
}

func TestGetBlockCountModel(t *testing.T) {
	assert := assert.New(t)

	blockCountModel := GetBlockCountModel()
	assert.NotEqual(nil, blockCountModel)
}

func TestBlockCountModelInsert(t *testing.T) {
	assert := assert.New(t)

	blockCountModel := GetBlockCountModel()
	assert.NotEqual(nil, blockCountModel)

	blockCountFixture := &models.BlockCount{
		Count: 1,
		Id:    10,
	}

	// Clear entry
	blockCountModel.Delete(*blockCountFixture)

	insertErr := blockCountModel.Insert(blockCountFixture)
	assert.Equal(nil, insertErr)
}

func TestBlockCountModelSelect(t *testing.T) {
	assert := assert.New(t)

	blockCountModel := GetBlockCountModel()
	assert.NotEqual(nil, blockCountModel)

	// Load fixture
	blockCountFixture := &models.BlockCount{
		Count: 1,
		Id:    10,
	}

	// Clear entry
	blockCountModel.Delete(*blockCountFixture)

	insertErr := blockCountModel.Insert(blockCountFixture)
	assert.Equal(nil, insertErr)

	// Select BlockCount
	result, err := blockCountModel.Select()
	assert.Equal(blockCountFixture.Count, result.Count)
	assert.Equal(blockCountFixture.Id, result.Id)
	assert.Equal(nil, err)
}

func TestBlockCountModelUpdate(t *testing.T) {
	assert := assert.New(t)

	blockCountModel := GetBlockCountModel()
	assert.NotEqual(nil, blockCountModel)

	// Load fixture
	blockCountFixture := &models.BlockCount{
		Count: 1,
		Id:    10,
	}

	// Clear entry
	blockCountModel.Delete(*blockCountFixture)

	insertErr := blockCountModel.Insert(blockCountFixture)
	assert.Equal(nil, insertErr)

	// Select BlockCount
	result, err := blockCountModel.Select()
	assert.Equal(blockCountFixture.Count, result.Count)
	assert.Equal(blockCountFixture.Id, result.Id)
	assert.Equal(nil, err)

	// Update BlockCount
	blockCountFixture = &models.BlockCount{
		Count: 10,
		Id:    10,
	}
	insertErr = blockCountModel.Update(blockCountFixture)
	assert.Equal(nil, insertErr)

	// Select BlockCount
	result, err = blockCountModel.Select()
	assert.Equal(blockCountFixture.Count, result.Count)
	assert.Equal(blockCountFixture.Id, result.Id)
	assert.Equal(nil, err)
}

func TestBlockCountModelLoader(t *testing.T) {
	assert := assert.New(t)

	blockCountModel := GetBlockCountModel()
	assert.NotEqual(nil, blockCountModel)

	// Load fixture
	blockCountFixture := &models.BlockCount{
		Count: 1,
		Id:    10,
	}

	// Clear entry
	blockCountModel.Delete(*blockCountFixture)

	// Start loader
	StartBlockCountLoader()

	// Write to loader channel
	go func() {
		for {
			blockCountModel.LoaderChannel <- blockCountFixture
			time.Sleep(1)
		}
	}()

	// Wait for inserts
	time.Sleep(5)
}
