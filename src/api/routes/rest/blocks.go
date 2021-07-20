package rest

import (
	"encoding/json"

	fiber "github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/config"
)

func BlocksAddHandlers(app *fiber.App) {

	prefix := config.Config.RestPrefix + "/blocks"

	app.Get(prefix+"/", handlerGetBlocks)
}

// Parameters for handlerGetBlocks
type paramsGetBlocks struct {
  Limit       int     `query:"limit"`
  Skip        int     `query:"skip"`
  Number      uint32  `query:"number"`
  StartNumber uint32  `query:"start_number"`
  EndNumber   uint32  `query:"end_number"`
  Hash        string  `query:"hash"`
  CreatedBy   string  `query:"created_by"`
}

// Blocks
// @Summary Get Blocks
// @Description get historical blocks
// @Tags Blocks
// @BasePath /api/v1
// @Accept */*
// @Produce json
// @Param limit query int false "1"
// @Param skip query int false "0"
// @Param number query int false "0"
// @Param start_number query int false "0"
// @Param end_number query int false "0"
// @Param hash query string false "\"\""
// @Param created_by query string false "\"\""
// @Router /api/v1/blocks [get]
// @Success 200 {object} []models.Block
// @Failure 422 {object} map[string]interface{}
func handlerGetBlocks(c *fiber.Ctx) error {
  params := &paramsGetBlocks{}
	if err := c.QueryParser(params); err != nil {
    zap.S().Warnf("Blocks Get Handler ERROR: %s", err.Error())

		c.Status(422)
    return c.SendString(`{"error": "could not parse query parameters"}`)
	}

  // Default params
  if params.Limit == 0 {
    params.Limit = 1
  }

  blocks := crud.GetBlockModel().Select(
    params.Limit,
    params.Skip,
    params.Number,
    params.StartNumber,
    params.EndNumber,
    params.Hash,
    params.CreatedBy,
  )

  body, _ := json.Marshal(blocks)
  return c.SendString(string(body))
}
