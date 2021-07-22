package kafka

import (
	"os"
	"testing"
	"time"

	"github.com/geometry-labs/icon-blocks/config"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gopkg.in/Shopify/sarama.v1"
)

func init() {
	config.ReadEnvironment()
}

func TestStartAPIConsumers(t *testing.T) {

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
	config.ReadEnvironment()

	StartAPIConsumers()

	// Wait for consumers
	time.Sleep(5 * time.Second)
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

func TestKafkaTopicConsumer(t *testing.T) {
	assert := assert.New(t)

	topicName := "mock-topic"

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

	// Start broadcaster
	newBroadcaster(topicName, nil)
	log.Info("TestKafkaTopicConsumer: Starting broadcaster...")
	go Broadcasters[topicName].Start()

	// Start consumer
	topicConsumer := &kafkaTopicConsumer{
		mockBroker.Addr(),
		topicName,
		Broadcasters[topicName],
	}
	log.Info("TestKafkaTopicConsumer: Starting consumer...")
	go topicConsumer.consumeTopic()

	// Add broadcasting channeld
	topicChan := make(chan *sarama.ConsumerMessage)
	broadcasterOutputChanID := Broadcasters[topicName].AddBroadcastChannel(topicChan)
	defer func() {
		Broadcasters[topicName].RemoveBroadcastChannel(broadcasterOutputChanID)
	}()

	for {
		select {
		case topicMsg := <-topicChan:
			// Pass
			assert.NotEqual(len(topicMsg.Value), 0)
			return
		case <-time.After(10 * time.Second):
			t.Fatal("No messages from consumer")
		}
	}
}
