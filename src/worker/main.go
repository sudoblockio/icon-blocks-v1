package main

import (
	"log"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/global"
	"github.com/geometry-labs/icon-blocks/kafka"
	"github.com/geometry-labs/icon-blocks/logging"
	"github.com/geometry-labs/icon-blocks/metrics"
	"github.com/geometry-labs/icon-blocks/worker/transformers"
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

	global.WaitShutdownSig()
}
