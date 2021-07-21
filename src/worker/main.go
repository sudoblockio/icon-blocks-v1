package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/geometry-labs/icon-blocks/worker/transformers"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/global"
	"github.com/geometry-labs/icon-blocks/kafka"
	"github.com/geometry-labs/icon-blocks/logging"
	"github.com/geometry-labs/icon-blocks/metrics"
)

func main() {
	config.ReadEnvironment()

	logging.Init()
	log.Printf("Main: Starting logging with level %s", config.Config.LogLevel)

	// Start Prometheus client
	metrics.WorkerStart()

	// Start kafka Producer
	// 3
	kafka.StartProducers()

	// Start Postgres loader
	// 4
	crud.StartBlockLoader()

	// Start kafka consumer
	// 1
	kafka.StartWorkerConsumers()

	// Start transformers
	// 2
	transformers.StartBlocksTransformer()

	//create a notification channel to shutdown
	sigChan := make(chan os.Signal, 1)

	// Listen for close sig
	// Register for interupt (Ctrl+C) and SIGTERM (docker)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		zap.S().Info("Shutting down...")
		global.ShutdownChan <- 1
	}()

	<-global.ShutdownChan
}
