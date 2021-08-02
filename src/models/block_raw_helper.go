package models

import (
	"math"
	"strconv"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"

	"encoding/hex"
)

// ConvertToBlockRawJSON - []byte -> models.BlockRaw in JSON format
func ConvertToBlockRawJSON(value []byte) (*BlockRaw, error) {
	block := BlockRaw{}
	err := protojson.Unmarshal(value, &block)
	if err != nil {
		zap.S().Error("Block_raw_helper: Error in ConvertToBlockRawJSON: %v", err)
	}
	return &block, err
}

// ConvertToBlockRawProtoBuf - []byte -> models.BlockRaw in ProtoBuf format
func ConvertToBlockRawProtoBuf(value []byte) (*BlockRaw, error) {
	block := BlockRaw{}
	err := proto.Unmarshal(value[7:], &block)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
		zap.S().Error("Value=", hex.Dump(value[7:]))
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
