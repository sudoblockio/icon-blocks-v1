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

	// Test limit
	blocks = blockModel.Select(1, 0, 0, 0, 0, "", "")
	assert.Equal(1, len(blocks))

	// Test skip
	blocks = blockModel.Select(1, 1, 0, 0, 0, "", "")
	assert.Equal(1, len(blocks))

	// Test number
	blocks = blockModel.Select(1, 0, 33788433, 0, 0, "", "")
	assert.Equal(1, len(blocks))

	// Test start_number
	blocks = blockModel.Select(1, 0, 0, 8150, 0, "", "")
	assert.Equal(1, len(blocks))

	// Test end_number
	blocks = blockModel.Select(1, 0, 0, 0, 8160, "", "")
	assert.Equal(1, len(blocks))

	// Test start_number <-> end_number
	blocks = blockModel.Select(1, 0, 0, 8150, 8160, "", "")
	assert.Equal(1, len(blocks))

	// Test hash
	blocks = blockModel.Select(1, 0, 0, 0, 0, "f2934304af91a2cecca184162dda895ab9929c28eddaee104cda988000824019", "")
	assert.Equal(1, len(blocks))

	// Test created_by (peer_id)
	blocks = blockModel.Select(1, 0, 0, 0, 0, "", "hx116e5ea176419cd990c2f39b0eda21b946728a38")
	assert.Equal(1, len(blocks))
}

func TestBlockModelCountAll(t *testing.T) {
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

	count := blockModel.CountAll()
	assert.NotEqual(0, count)
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
