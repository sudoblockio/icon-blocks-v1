//+build unit

package metrics

import (
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/geometry-labs/icon-blocks/config"

	"github.com/stretchr/testify/assert"
)

func TestMetricsApiStart(t *testing.T) {
	assert := assert.New(t)

	// Set env
	os.Setenv("METRICS_PORT", "8888")
	os.Setenv("METRICS_PREFIX", "/metrics")

	config.ReadEnvironment()

	// Start metrics server
	MetricsApiStart()

	metrics["requests_amount"].Inc()
	metrics["kafka_messages_consumed"].Inc()
	metrics["websockets_connected"].Inc()
	metrics["websockets_bytes_written"].Inc()

	resp, err := http.Get(fmt.Sprintf("http://localhost:%s%s", config.Config.MetricsPort, config.Config.MetricsPrefix))
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)
}
