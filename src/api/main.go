package main

import (
	"log"

	"github.com/geometry-labs/icon-blocks/api/healthcheck"
	"github.com/geometry-labs/icon-blocks/api/routes"
	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/global"
	"github.com/geometry-labs/icon-blocks/logging"
	"github.com/geometry-labs/icon-blocks/metrics"
	_ "github.com/geometry-labs/icon-blocks/models" // for swagger docs
	"github.com/geometry-labs/icon-blocks/redis"
)

func main() {
	config.ReadEnvironment()

	logging.Init()
	log.Printf("Main: Starting logging with level %s", config.Config.LogLevel)

	// Start Prometheus client
	// Go routine starts in function
	metrics.APIStart()

	// Start Redis Client
	// NOTE: redis is used for websockets
	redis.GetBroadcaster().Start()
	redis.GetRedisClient().StartSubscriber()

	// Start API server
	// Go routine starts in function
	routes.Start()

	// Start Health server
	// Go routine starts in function
	healthcheck.Start()

	global.WaitShutdownSig()
}
