package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-blocks/config"
)

// Metrics
var (
	MaxBlockNumberBlocksRawGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name:        "max_block_number_blocks_raw",
		Help:        "max block number read from the blocks_raw topic",
		ConstLabels: prometheus.Labels{"network_name": config.Config.NetworkName},
	})
	MaxBlockNumberTransactionsRawGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name:        "max_block_number_transactions_raw",
		Help:        "max block number read from the transactions_raw topic",
		ConstLabels: prometheus.Labels{"network_name": config.Config.NetworkName},
	})
	MaxBlockNumberLogsRawGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name:        "max_block_number_logs_raw",
		Help:        "max block number read from the logs_raw topic",
		ConstLabels: prometheus.Labels{"network_name": config.Config.NetworkName},
	})
)

func Start() {

	// Start server
	http.Handle(config.Config.MetricsPrefix, promhttp.Handler())
	go http.ListenAndServe(":"+config.Config.MetricsPort, nil)
	zap.S().Info("Started Metrics:", config.Config.MetricsPort)
}
