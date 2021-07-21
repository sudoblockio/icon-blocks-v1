//+build unit

package healthcheck

import (
	"net/http"
	"testing"

	"github.com/geometry-labs/icon-blocks/config"

	"github.com/stretchr/testify/assert"

	"github.com/geometry-labs/icon-blocks/api/routes"
)

func init() {
	config.ReadEnvironment()
}

func TestHealthCheck(t *testing.T) {
	assert := assert.New(t)

	// Start api
	routes.Start()

	// Start healthcheck
	Start()

	resp, err := http.Get("http://localhost:" + config.Config.HealthPort + config.Config.HealthPrefix)
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)
}
