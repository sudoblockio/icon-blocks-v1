package crud

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/fixtures"
	"github.com/geometry-labs/icon-blocks/logging"
)

func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()

	// Set up logging
	logging.Init()
}

func TestGetBlockModel(t *testing.T) {
	assert := assert.New(t)

	blockModel := GetBlockModel()
	assert.NotEqual(nil, blockModel)
}

func TestBlockModelMigrate(t *testing.T) {
	assert := assert.New(t)

	blockModel := GetBlockModel()
	assert.NotEqual(nil, blockModel)

	migrateErr := blockModel.Migrate()
	assert.Equal(nil, migrateErr)
}

func TestBlockModelInsert(t *testing.T) {
	assert := assert.New(t)

	blockModel := GetBlockModel()
	assert.NotEqual(nil, blockModel)

	migrateErr := blockModel.Migrate()
	assert.Equal(nil, migrateErr)

	// Load fixtures
	blockFixtures := fixtures.LoadBlockFixtures()

	for _, block := range blockFixtures {
		insertErr := blockModel.Insert(block)
		assert.Equal(nil, insertErr)
	}
}

func TestBlockModelSelect(t *testing.T) {
	assert := assert.New(t)

	blockModel := GetBlockModel()
	assert.NotEqual(nil, blockModel)

	migrateErr := blockModel.Migrate()
	assert.Equal(nil, migrateErr)

	// Load fixtures
	blockFixtures := fixtures.LoadBlockFixtures()
	for _, block := range blockFixtures {
		insertErr := blockModel.Insert(block)
		assert.Equal(nil, insertErr)
	}

	// Select all blocks
	blocks := blockModel.Select(len(blockFixtures), 0, 0, 0, 0, "", "")
	assert.Equal(len(blockFixtures), len(blocks))
}

func TestBlockModelLoader(t *testing.T) {
	assert := assert.New(t)

	blockModel := GetBlockModel()
	assert.NotEqual(nil, blockModel)

	migrateErr := blockModel.Migrate()
	assert.Equal(nil, migrateErr)

	// Load fixtures
	blockFixtures := fixtures.LoadBlockFixtures()

	// Start loader
	go StartBlockLoader()

	// Write to loader channel
	go func() {
		for _, fixture := range blockFixtures {
			blockModel.WriteChan <- fixture
		}
	}()

	// Wait for inserts
	time.Sleep(5)

	// Select all blocks
	blocks := blockModel.Select(len(blockFixtures), 0, 0, 0, 0, "", "")
	assert.Equal(len(blockFixtures), len(blocks))
}
