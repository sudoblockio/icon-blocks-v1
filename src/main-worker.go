package main

import (
	"os"
	"os/signal"
	"syscall"
  "time"
  "log"

	"github.com/geometry-labs/icon-blocks/worker/transformers"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/global"
	"github.com/geometry-labs/icon-blocks/kafka"
	"github.com/geometry-labs/icon-blocks/logging"
	"github.com/geometry-labs/icon-blocks/metrics"
	"github.com/geometry-labs/icon-blocks/worker/loader"
)

func main() {
	config.ReadEnvironment()

	logging.StartLoggingInit()
	log.Printf("Main: Starting logging with level %s", config.Config.LogLevel)

	// Start Prometheus client
	metrics.MetricsWorkerStart()

	// Start kafka consumer
	kafka.StartWorkerConsumers()

	// Start kafka Producer
	kafka.StartProducers()

	// Start Postgres loader
	loader.StartBlockLoader()

	// Start transformers
	transformers.StartBlocksTransformer()

	// Listen for close sig
	// Register for interupt (Ctrl+C) and SIGTERM (docker)

	//create a notification channel to shutdown
	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		zap.S().Info("Shutting down...")
		global.ShutdownChan <- 1
	}()

	<-global.ShutdownChan
}
