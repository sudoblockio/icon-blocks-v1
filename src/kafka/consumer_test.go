package kafka

import (
	"os"
	"testing"
	"time"

	"github.com/geometry-labs/icon-blocks/config"

	log "github.com/sirupsen/logrus"
	"gopkg.in/Shopify/sarama.v1"
)

func init() {
	config.ReadEnvironment()
}

func TestStartWorkerConsumers(t *testing.T) {

	topicName := "blocks"

	// Mock broker
	mockBrokerID := int32(1)
	mockBroker := sarama.NewMockBroker(t, mockBrokerID)
	defer mockBroker.Close()

	mockBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(mockBroker.Addr(), mockBroker.BrokerID()).
			SetLeader(topicName, 0, mockBroker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(topicName, 0, sarama.OffsetOldest, 0).
			SetOffset(topicName, 0, sarama.OffsetNewest, 2),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).
			SetMessage(topicName, 0, 0, sarama.ByteEncoder([]byte{0x41, 0x42})).
			SetMessage(topicName, 0, 1, sarama.ByteEncoder([]byte{0x41, 0x43})).
			SetMessage(topicName, 0, 2, sarama.ByteEncoder([]byte{0x41, 0x44})),
	})

	mockBroker.SetNotifier(func(bytes_read int, bytes_written int) {
		log.Debug("MOCK NOTIFIER: bytes_read=", bytes_read, " bytes_written=", bytes_written)
	})

	// Set config
	os.Setenv("KAFKA_BROKER_URL", mockBroker.Addr())
	os.Setenv("CONSUMER_TOPICS", topicName)
	os.Setenv("CONSUMER_GROUP", "test-consumer-group")
	config.ReadEnvironment()

	StartWorkerConsumers()

	// Wait for consumers
	time.Sleep(5 * time.Second)
}
