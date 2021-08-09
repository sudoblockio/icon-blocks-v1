package rest

import (
  "strconv"
	"encoding/json"

	fiber "github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
)

// BlocksAddHandlers - add blocks endpoints to fiber router
func BlocksAddHandlers(app *fiber.App) {

	prefix := config.Config.RestPrefix + "/blocks"

	app.Get(prefix+"/", handlerGetBlocks)
}

// Parameters for handlerGetBlocks
type paramsGetBlocks struct {
	Limit       int    `query:"limit"`
	Skip        int    `query:"skip"`
	Number      uint32 `query:"number"`
	StartNumber uint32 `query:"start_number"`
	EndNumber   uint32 `query:"end_number"`
	Hash        string `query:"hash"`
	CreatedBy   string `query:"created_by"`
}

// Blocks
// @Summary Get Blocks
// @Description get historical blocks
// @Tags Blocks
// @BasePath /api/v1
// @Accept */*
// @Produce json
// @Param limit query int false "amount of records"
// @Param skip query int false "skip to a record"
// @Param number query int false "find by block number"
// @Param start_number query int false "range by start block number"
// @Param end_number query int false "range by end block number"
// @Param hash query string false "find by block hash"
// @Param created_by query string false "find by block creator"
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

  // Set headers
  c.Append("X-TOTAL-COUNT", strconv.Itoa(len(blocks)))

	body, _ := json.Marshal(&blocks)
	return c.SendString(string(body))
}
