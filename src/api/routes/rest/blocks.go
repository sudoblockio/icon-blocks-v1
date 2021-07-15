package rest

import (
	"encoding/json"

	fiber "github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-blocks/api/service"
	"github.com/geometry-labs/icon-blocks/config"
)

func BlocksAddHandlers(app *fiber.App) {

	prefix := config.Config.RestPrefix + "/blocks"

	app.Get(prefix+"/", handlerGetQuery)
}

// Blocks
// @Summary Get Blocks that match the query
// @Description Get all blocks in the system.
// @Tags root
// @Accept */*
// @Produce json
// @Router /blocks [get]
func handlerGetQuery(c *fiber.Ctx) error {
	params := new(service.BlocksQueryService)
	if err := c.QueryParser(params); err != nil {
    zap.S().Warnf("Blocks Get Handler ERROR: %s", err.Error())

		c.Status(422)
    return c.SendString(`{"error": "could not parse query parameters"}`)
	}

  // Get Blocks
	blocks := params.RunQuery()
	if len(*blocks) == 0 {
    // No Content
		c.Status(204)
	} else {
    // Success
	  c.Status(200)
  }

	body, _ := json.Marshal(blocks)
	return c.SendString(string(body))
}
