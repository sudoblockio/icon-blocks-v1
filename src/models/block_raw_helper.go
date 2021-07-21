package models

import (
	"math"
	"strconv"

	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
)

// ConvertToBlockRaw - []byte -> models.BlockRaw
func ConvertToBlockRaw(value []byte) (*BlockRaw, error) {
	block := BlockRaw{}
	err := protojson.Unmarshal(value, &block)
	if err != nil {
		zap.S().Error("Block_raw_helper: Error in ConvertToBlockRaw: %v", err)
	}
	return &block, err
}

// ValidateHeight - string -> int
func ValidateHeight(heightString string) bool {
	height, err := strconv.Atoi(heightString)
	if err != nil {
		return false
	}
	if height < 0 || height > math.MaxUint32 {
		return false
	}
	return true
}
