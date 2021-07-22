package fixtures

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/logging"
)

func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()

	// Set up logging
	logging.Init()
}

func TestLoadBlockFixtures(t *testing.T) {
	assert := assert.New(t)

	blockFixtures := LoadBlockFixtures()

	assert.NotEqual(0, len(blockFixtures))
}
