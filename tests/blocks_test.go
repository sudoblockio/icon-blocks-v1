package tests

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlocksEndpoint(t *testing.T) {
	assert := assert.New(t)

	blocksServiceURL := os.Getenv("BLOCKS_SERVICE_URL")
	if blocksServiceURL == "" {
		blocksServiceURL = "http://localhost:8000"
	}
	blocksServiceRestPrefx := os.Getenv("BLOCKS_SERVICE_REST_PREFIX")
	if blocksServiceRestPrefx == "" {
		blocksServiceRestPrefx = "/api/v1"
	}

	resp, err := http.Get(blocksServiceURL + blocksServiceRestPrefx + "/blocks")
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)

	// Verify body
	assert.NotEqual(0, len(bodyMap))
}
