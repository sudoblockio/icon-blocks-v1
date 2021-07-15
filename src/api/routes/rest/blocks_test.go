//+build integration

package rest

import (
	"encoding/json"
	"io/ioutil"
	"net/http/httptest"
	"testing"

	fiber "github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/models"
)

func init() {
	config.ReadEnvironment()
}

func TestHandlerGetBlocks(t *testing.T) {
	assert := assert.New(t)

  // Insert block fixtures
  block := *models.Blocks{}
  global.GetGlobal().Blocks.RetryCreate(block)

	app := fiber.New()

	app.Get("/", handlerGetQuery)

	resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	// Read body
	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	var blocks []models.Block
	err = json.Unmarshal(bytes, &blocks)
	assert.Equal(nil, err)

	// Verify body
	assert.NotEqual(0, len(blocks[0].Hash))
}

