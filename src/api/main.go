package main

import (
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

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
	zap.S().Debug("Main: Starting logging with level ", config.Config.LogLevel)

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

	//create a notification channel to shutdown
	sigChan := make(chan os.Signal, 1)

	// Listen for close sig
	// Register for interupt (Ctrl+C) and SIGTERM (docker)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		zap.S().Info("Main: Shutting down...")
		global.ShutdownChan <- 1
	}()

	<-global.ShutdownChan
}
