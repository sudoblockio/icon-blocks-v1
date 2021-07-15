package tests

import (
  "os"
  "testing"
  "net/http"
  "encoding/json"
  "io/ioutil"

	"github.com/stretchr/testify/assert"
)


func TestMetadataEndpoint(t *testing.T) {
  assert := assert.New(t)

  blocks_service_url := os.Getenv("BLOCKS_SERVICE_URL")
  if blocks_service_url == "" {
    blocks_service_url = "http://localhost:8000"
  }

  resp, err := http.Get(blocks_service_url + "/metadata")
  assert.Equal(nil, err)
  assert.Equal(200, resp.StatusCode)

  defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	body_map := make(map[string]interface{})
	err = json.Unmarshal(bytes, &body_map)
	assert.Equal(nil, err)

	// Verify body
	assert.NotEqual(0, len(body_map["version"].(string)))
}
