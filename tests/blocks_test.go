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

// List test
func TestBlocksEndpointList(t *testing.T) {
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

	// Test headers
	assert.NotEqual("0", resp.Header.Get("X-TOTAL-COUNT"))

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))
}

// List limit and skip test
func TestBlocksEndpointListLimitSkip(t *testing.T) {
	assert := assert.New(t)

	blocksServiceURL := os.Getenv("BLOCKS_SERVICE_URL")
	if blocksServiceURL == "" {
		blocksServiceURL = "http://localhost:8000"
	}
	blocksServiceRestPrefx := os.Getenv("BLOCKS_SERVICE_REST_PREFIX")
	if blocksServiceRestPrefx == "" {
		blocksServiceRestPrefx = "/api/v1"
	}

	resp, err := http.Get(blocksServiceURL + blocksServiceRestPrefx + "/blocks?limit=100&skip=100")
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
}

// List number test
func TestBlocksEndpointListNumber(t *testing.T) {
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

	blockNumber := uint64(bodyMap[0].(map[string]interface{})["number"].(float64))

	// Test number
	resp, err = http.Get(blocksServiceURL + blocksServiceRestPrefx + "/blocks?number=" + strconv.FormatUint(blockNumber, 10))
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	// Test headers
	assert.NotEqual("0", resp.Header.Get("X-TOTAL-COUNT"))

	bytes, err = ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap = make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))
}
