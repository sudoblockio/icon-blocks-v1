package kafka

import (
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/geometry-labs/icon-blocks/config"
	"go.uber.org/zap"
	"gopkg.in/Shopify/sarama.v1"
)

type kafkaTopicProducer struct {
	brokerURL string
	topicName string
	topicChan chan *sarama.ProducerMessage
}

// map[Topic_Name] -> Producer
var kafkaTopicProducers = map[string]*kafkaTopicProducer{}

// StartProducers - start goroutine to produce to kafka
func StartProducers() {
	kafkaBrokerURL := config.Config.KafkaBrokerURL
	producerTopics := config.Config.ProducerTopics

	zap.S().Info("Start Producer: kafkaBrokerURL=", kafkaBrokerURL, " producerTopics=", producerTopics)

	for _, t := range producerTopics {
		// Todo: parameterize schema
		schema := "block" //config.Config.SchemaNameTopics["block-ws"] //"block"
		_, err := RetriableRegisterSchema(RegisterSchema, t, false, "block", true)
		if err != nil {
			zap.S().Error(fmt.Sprintf("Error in registering schema: %s for topic: %s", schema, t))
			continue
		}
		zap.S().Info(fmt.Sprintf("Registered schema: %s for topic: %s", schema, t))

		kafkaTopicProducers[t] = &kafkaTopicProducer{
			kafkaBrokerURL,
			t,
			make(chan *sarama.ProducerMessage),
		}

		go kafkaTopicProducers[t].produceTopic()
	}
}

func (k *kafkaTopicProducer) produceTopic() {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Return.Successes = true

	producer, err := getProducer(k, saramaConfig)
	if err != nil {
		zap.S().Info("KAFKA PRODUCER NEWSYNCPRODUCER: Finally Connection cannot be established")
	} else {
		zap.S().Info("KAFKA PRODUCER NEWSYNCPRODUCER: Finally Connection established")
	}
	defer func() {
		if err := producer.Close(); err != nil {
			zap.S().Panic("KAFKA PRODUCER CLOSE PANIC: ", err.Error())
		}
	}()

	zap.S().Debug("Producer ", k.topicName, ": Started producing")
	for {
		topicMsg := <-k.topicChan

		partition, offset, err := producer.SendMessage(topicMsg)
		if err != nil {
			zap.S().Warn("Producer ", k.topicName, ": Err sending message=", err.Error())
		}

		zap.S().Debug("Producer ", k.topicName, ": Producing message partition=", partition, " offset=", offset)
	}
}

func getProducer(k *kafkaTopicProducer, saramaConfig *sarama.Config) (sarama.SyncProducer, error) {
	var producer sarama.SyncProducer
	operation := func() error {
		pro, err := sarama.NewSyncProducer([]string{k.brokerURL}, saramaConfig)
		if err != nil {
			zap.S().Info("KAFKA PRODUCER NEWSYNCPRODUCER PANIC: ", err.Error())
		} else {
			producer = pro
		}
		return err
	}
	neb := backoff.NewExponentialBackOff()
	neb.MaxElapsedTime = time.Minute
	err := backoff.Retry(operation, neb)
	return producer, err
}
