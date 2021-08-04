package main

import (
	"log"

	"github.com/geometry-labs/icon-blocks/api/healthcheck"
	"github.com/geometry-labs/icon-blocks/api/routes"
	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/global"
	"github.com/geometry-labs/icon-blocks/kafka"
	"github.com/geometry-labs/icon-blocks/logging"
	"github.com/geometry-labs/icon-blocks/metrics"
	_ "github.com/geometry-labs/icon-blocks/models" // for swagger docs
)

func main() {
	config.ReadEnvironment()

	logging.Init()
	log.Printf("Main: Starting logging with level %s", config.Config.LogLevel)

	// Start kafka consumers
	// Go routines start in function
	kafka.StartAPIConsumers()

	// Start Prometheus client
	// Go routine starts in function
	metrics.APIStart()

	// Start API server
	// Go routine starts in function
	routes.Start()

	// Start Health server
	// Go routine starts in function
	healthcheck.Start()

	global.WaitShutdownSig()
}
