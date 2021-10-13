package routes

import (
	"encoding/json"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/global"
	"go.uber.org/zap"

	swagger "github.com/arsmn/fiber-swagger/v2"
	fiber "github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"

	_ "github.com/geometry-labs/icon-blocks/api/docs" // import for swagger docs
	"github.com/geometry-labs/icon-blocks/api/routes/rest"
	"github.com/geometry-labs/icon-blocks/api/routes/ws"
)

// Start - start fiber server
// @title Go api template docs
// @version 2.0
// @description This is a sample server server.
// @host localhost:8000
// @BasePath /
func Start() {

	app := fiber.New()

	// Logging middleware
	app.Use(func(c *fiber.Ctx) error {
		zap.S().Info(c.Method(), " ", c.Path())

		// Go to next middleware:
		return c.Next()
	})

	// CORS Middleware
	app.Use(cors.New(cors.Config{
		AllowOrigins: config.Config.CORSAllowOrigins,
		AllowHeaders: config.Config.CORSAllowHeaders,
	}))

	// Swagger docs
	app.Get(config.Config.RestPrefix+"/blocks/docs/*", swagger.Handler)

	// Add version handlers
	app.Get("/version", handlerVersion)
	app.Get("/metadata", handlerMetadata)

	// Add handlers
	rest.BlocksAddHandlers(app)
	ws.BlocksAddHandlers(app)

	go app.Listen(":" + config.Config.Port)
}

// Version
// @Summary Show the status of server.
// @Description get the status of server.
// @Tags Version
// @Accept */*
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /version [get]
func handlerVersion(c *fiber.Ctx) error {
	message := map[string]string{
		"version": global.Version,
	}

	jsonMessage, _ := json.Marshal(message)

	return c.SendString(string(jsonMessage))
}

// Metadata
// @Summary Show the status of server.
// @Description get the status of server.
// @Tags Version
// @Accept */*
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /metadata [get]
func handlerMetadata(c *fiber.Ctx) error {
	message := map[string]string{
		"version":     global.Version,
		"name":        config.Config.Name,
		"description": "a go api template",
	}

	jsonMessage, _ := json.Marshal(message)

	return c.SendString(string(jsonMessage))
}
