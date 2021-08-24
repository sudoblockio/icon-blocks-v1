package kafka

import (
	"testing"
	"time"

	"github.com/geometry-labs/icon-blocks/config"

	log "github.com/sirupsen/logrus"
	"gopkg.in/Shopify/sarama.v1"
)

func init() {
	config.ReadEnvironment()
}

func TestKafkaTopicProducer(t *testing.T) {

	topicName := "mock-topic"

	// Mock broker
	mockBrockerID := int32(1)
	mockBroker := sarama.NewMockBroker(t, mockBrockerID)
	defer mockBroker.Close()

	mockBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(mockBroker.Addr(), mockBroker.BrokerID()).
			SetLeader(topicName, 0, mockBroker.BrokerID()),
		"ProduceRequest": sarama.NewMockProduceResponse(t),
	})

	KafkaTopicProducers[topicName] = &kafkaTopicProducer{
		mockBroker.Addr(),
		topicName,
		make(chan *sarama.ProducerMessage),
	}

	go KafkaTopicProducers[topicName].produceTopic()

	msgKey := "KEY"
	msgValue := "VALUE"
	go func() {
		for {
			KafkaTopicProducers[topicName].TopicChan <- &sarama.ProducerMessage{
				Topic:     topicName,
				Partition: -1,
				Key:       sarama.StringEncoder(msgKey),
				Value:     sarama.StringEncoder(msgValue),
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	totalBytesRead := 0
	totalBytesWritten := 0
	mockBroker.SetNotifier(func(bytesRead int, bytesWritten int) {
		log.Debug("MOCK NOTIFIER: bytesRead=", bytesRead, " bytesWritten=", bytesWritten)

		totalBytesRead += bytesRead
		totalBytesWritten += bytesWritten
	})

	loops := 0
	for {
		if loops > 10 {
			t.Fatal("No messages to mock broker")
		}
		if totalBytesRead > 400 && totalBytesWritten > 200 {
			// Test passed
			return
		}

		time.Sleep(time.Second)
		loops++
	}
}
