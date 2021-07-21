package metrics

import (
	"net/http"

	"github.com/geometry-labs/icon-blocks/config"
	"go.uber.org/zap"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var metrics map[string]prometheus.Counter

// APIStart - start metrics server with API config
func APIStart() {
	metrics = make(map[string]prometheus.Counter)

	createAPIGauges()

	// Start server
	http.Handle(config.Config.MetricsPrefix, promhttp.Handler())
	go http.ListenAndServe(":"+config.Config.MetricsPort, nil)
	zap.S().Info("Started Metrics:", config.Config.MetricsPort)
}

// WorkerStart - start metrics server with Worker config
func WorkerStart() {
	metrics = make(map[string]prometheus.Counter)

	// create gauges
	createWorkerGauges()

	// Start server
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":"+config.Config.MetricsPort, nil)
	zap.S().Info("Started Metrics on port:", config.Config.MetricsPort, config.Config.MetricsPrefix)
}

func createAPIGauges() {
	metrics["requests_amount"] = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "requests_amount",
		Help:        "amount of requests",
		ConstLabels: prometheus.Labels{"network_name": config.Config.NetworkName},
	})
	metrics["kafka_messages_consumed"] = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "kafka_messages_consumed",
		Help:        "amount of messageds from kafka consumed",
		ConstLabels: prometheus.Labels{"network_name": config.Config.NetworkName},
	})
	metrics["websockets_connected"] = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "websockets_connected",
		Help:        "amount of websockets that have connected to the server",
		ConstLabels: prometheus.Labels{"network_name": config.Config.NetworkName},
	})
	metrics["websockets_bytes_written"] = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "websockets_bytes_written",
		Help:        "amount of bytes written through websockets",
		ConstLabels: prometheus.Labels{"network_name": config.Config.NetworkName},
	})
}

func createWorkerGauges() {
	metrics["kafka_messages_consumed"] = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "kafka_messages_consumed",
		Help:        "amount of messages from kafka consumed",
		ConstLabels: prometheus.Labels{"network_name": config.Config.NetworkName},
	})
	metrics["kafka_messages_produced"] = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "kafka_messages_produced",
		Help:        "amount of messages from kafka produced",
		ConstLabels: prometheus.Labels{"network_name": config.Config.NetworkName},
	})
}
