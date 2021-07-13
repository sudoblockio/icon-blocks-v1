package rest

import (
	"encoding/json"
	"github.com/geometry-labs/icon-blocks/config"
	"io/ioutil"
	"net/http/httptest"
	"testing"

	fiber "github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
)

func init() {
	config.ReadEnvironment()
}

func TestHandlerGetBlock(t *testing.T) {
	assert := assert.New(t)

	app := fiber.New()

	app.Get("/", handlerGetQuery)

	resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	// Read body
	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	body_map := make(map[string]interface{})
	err = json.Unmarshal(bytes, &body_map)
	assert.Equal(nil, err)

	// Verify body
	assert.NotEqual(0, len(body_map["block"].(string)))
}
