package fixtures

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-blocks/models"
)

const (
	blockRawFixturesPath = "blocks_raw.json"
)

// Fixtures - slice of Fixture
type Fixtures []Fixture

// Fixture - loaded from fixture file
type Fixture map[string]interface{}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// LoadBlockFixtures - load block fixtures from disk
func LoadBlockFixtures() []*models.Block {
	blocks := make([]*models.Block, 0)

	fixtures, err := loadFixtures(blockRawFixturesPath)
	check(err)

	for _, fixture := range fixtures {
		blocks = append(blocks, parseFixtureToBlock(fixture))
	}

	return blocks
}

func loadFixtures(file string) (Fixtures, error) {
	var fs Fixtures

	dat, err := ioutil.ReadFile(getFixtureDir() + file)
	check(err)
	err = json.Unmarshal(dat, &fs)

	return fs, err
}

func getFixtureDir() string {

	callDir, _ := os.Getwd()
	callDirSplit := strings.Split(callDir, "/")

	for i := len(callDirSplit) - 1; i >= 0; i-- {
		if callDirSplit[i] != "src" {
			callDirSplit = callDirSplit[:len(callDirSplit)-1]
		} else {
			break
		}
	}

	callDirSplit = append(callDirSplit, "fixtures")
	fixtureDir := strings.Join(callDirSplit, "/")
	fixtureDir = fixtureDir + "/"
	zap.S().Info(fixtureDir)

	return fixtureDir
}

func parseFixtureToBlock(data map[string]interface{}) *models.Block {

	return &models.Block{
		Signature:        data["signature"].(string),
		ItemId:           data["item_id"].(string),
		NextLeader:       data["next_leader"].(string),
		TransactionCount: uint32(data["transaction_count"].(float64)),
		Type:             data["type"].(string),
		Version:          data["version"].(string),
		PeerId:           data["peer_id"].(string),
		Number:           uint32(data["number"].(float64)),
		MerkleRootHash:   data["merkle_root_hash"].(string),
		ItemTimestamp:    data["item_timestamp"].(string),
		Hash:             data["hash"].(string),
		ParentHash:       data["parent_hash"].(string),
		Timestamp:        uint64(data["timestamp"].(float64)),
	}
}
