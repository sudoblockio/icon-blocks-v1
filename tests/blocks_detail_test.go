package tests

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// List number test
func TestBlocksEndpointDetail(t *testing.T) {
	assert := assert.New(t)

	blocksServiceURL := os.Getenv("BLOCKS_SERVICE_URL")
	if blocksServiceURL == "" {
		blocksServiceURL = "http://localhost:8000"
	}
	blocksServiceRestPrefx := os.Getenv("BLOCKS_SERVICE_REST_PREFIX")
	if blocksServiceRestPrefx == "" {
		blocksServiceRestPrefx = "/api/v1"
	}

	// Get latest block
	resp, err := http.Get(blocksServiceURL + blocksServiceRestPrefx + "/blocks?limit=1")
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	// Test headers
	assert.NotEqual("0", resp.Header.Get("X-TOTAL-COUNT"))

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))

	// Get testable number
	blockNumber := strconv.FormatUint(uint64(bodyMap[0].(map[string]interface{})["number"].(float64)), 10)

	// Test number
	resp, err = http.Get(blocksServiceURL + blocksServiceRestPrefx + "/blocks/" + blockNumber)
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	bytes, err = ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMapResult := make(map[string]interface{})
	err = json.Unmarshal(bytes, &bodyMapResult)
	assert.Equal(nil, err)

	blockNumberResult := strconv.FormatUint(uint64(bodyMapResult["number"].(float64)), 10)
	assert.Equal(blockNumber, blockNumberResult)
}
