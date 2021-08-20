package kafka

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gopkg.in/Shopify/sarama.v1"

	"github.com/geometry-labs/icon-blocks/config"
)

// StartAPIConsumers - start consumer goroutines for API config
func StartAPIConsumers() {
	kafkaBroker := config.Config.KafkaBrokerURL
	consumerTopics := config.Config.ConsumerTopics

	zap.S().Debug("Start Consumer: kafkaBroker=", kafkaBroker, " consumerTopics=", consumerTopics)

	BroadcastFunc := func(channel chan *sarama.ConsumerMessage, message *sarama.ConsumerMessage) {
		select {
		case channel <- message:
			return
		default:
			return
		}
	}

	for _, t := range consumerTopics {
		// Starts go routine
		newBroadcaster(t, BroadcastFunc)

		topicConsumer := &kafkaTopicConsumer{
			kafkaBroker,
			t,
			Broadcasters[t],
		}

		// One routine per topic
		zap.S().Debug("Start Consumers: Starting ", t, " consumer...")
		go topicConsumer.consumeTopic()
	}
}

// StartWorkerConsumers - start consumer goroutines for Worker config
func StartWorkerConsumers() {
	kafkaBroker := config.Config.KafkaBrokerURL
	consumerTopics := config.Config.ConsumerTopics
	consumerGroup := config.Config.ConsumerGroup

	for _, t := range consumerTopics {
		// Starts go routine
		newBroadcaster(t, nil)

		topicConsumer := &kafkaTopicConsumer{
			kafkaBroker,
			t,
			Broadcasters[t],
		}

		// One routine per topic
		go topicConsumer.consumeGroup(consumerGroup)

		zap.S().Info("Start Consumer: kafkaBroker=", kafkaBroker, " consumerTopics=", t, " consumerGroup=", consumerGroup)
	}
}

type kafkaTopicConsumer struct {
	brokerURL   string
	topicName   string
	broadcaster *TopicBroadcaster
}

// Used internally by Sarama
func (*kafkaTopicConsumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (*kafkaTopicConsumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (k *kafkaTopicConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		var topicMsg *sarama.ConsumerMessage
		select {
		case msg := <-claim.Messages():
			topicMsg = msg
		case <-time.After(5 * time.Second):
			zap.S().Debug("Consumer ", k.topicName, ": No new kafka messages, waited 5 secs")
			continue
		}

		zap.S().Debug("New Kafka Consumer Group Message: offset=", topicMsg.Offset, " key=", string(topicMsg.Key))

		// Commit offset
		sess.MarkMessage(topicMsg, "")

		// Broadcast
		k.broadcaster.ConsumerChan <- topicMsg

		zap.S().Debug("Consumer ", k.topicName, ": Broadcasted message key=", string(topicMsg.Key))
	}
	return nil
}

func (k *kafkaTopicConsumer) consumeGroup(group string) {
	version, err := sarama.ParseKafkaVersion("2.1.1")
	if err != nil {
		zap.S().Panic("CONSUME GROUP ERROR: parsing Kafka version: ", err.Error())
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = version
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup([]string{k.brokerURL}, group, saramaConfig)
	if err != nil {
		zap.S().Panic("CONSUME GROUP ERROR: creating consumer group client: ", err.Error())
	}

	// From example: /sarama/blob/master/examples/consumergroup/main.go
	go func() {
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			err := client.Consume(ctx, []string{k.topicName}, k)
			if err != nil {
				zap.S().Warn("CONSUME GROUP ERROR: from consumer: ", err.Error())
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				zap.S().Warn("CONSUME GROUP WARN: from context: ", ctx.Err().Error())
				return
			}
		}
	}()

	// Waiting, so that client remains alive
	ch := make(chan int, 1)
	<-ch
	cancel()
}

func (k *kafkaTopicConsumer) consumeTopic() {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true

	// Connect consumer on Retry
	var consumer sarama.Consumer
	err := backoff.Retry(func() error {
		var err error
		consumer, err = sarama.NewConsumer([]string{k.brokerURL}, saramaConfig)
		if err != nil {
			zap.S().Warn("Kafka New Consumer Error: ", err.Error())
			zap.S().Warn("Cannot connect to kafka broker retrying...")
			return err
		}

		return nil
	}, backoff.NewExponentialBackOff())

	if err != nil {
		zap.S().Panic("KAFKA CONSUMER NEWCONSUMER PANIC: ", err.Error())
	}

	zap.S().Debug("Kakfa Consumer: Broker connection established")
	defer func() {
		if err := consumer.Close(); err != nil {
			zap.S().Warn("KAFKA CONSUMER CLOSE: ", err.Error())
		}
	}()

	offset := sarama.OffsetNewest
	partitions, err := consumer.Partitions(k.topicName)
	if err != nil {
		zap.S().Panic("KAFKA CONSUMER PARTITIONS PANIC: ", err.Error(), " Topic: ", k.topicName)
	}

	zap.S().Debug("Consumer ", k.topicName, ": Started consuming")
	for _, p := range partitions {
		pc, err := consumer.ConsumePartition(k.topicName, p, offset)
		defer func() {
			err = pc.Close()
			if err != nil {
				zap.S().Warn("PARTITION CONSUMER CLOSE: ", err.Error())
			}
		}()

		if err != nil {
			zap.S().Panic("KAFKA CONSUMER PARTITIONS PANIC: ", err.Error())
		}
		if pc == nil {
			zap.S().Panic("KAFKA CONSUMER PARTITIONS PANIC: Failed to create PartitionConsumer")
		}

		// One routine per partition
		go func(pc sarama.PartitionConsumer) {
			for {
				var topicMsg *sarama.ConsumerMessage
				select {
				case msg := <-pc.Messages():
					topicMsg = msg
				case consumerErr := <-pc.Errors():
					zap.S().Warn("KAFKA PARTITION CONSUMER ERROR:", consumerErr.Err)
					continue
				case <-time.After(5 * time.Second):
					zap.S().Debug("Consumer ", k.topicName, ": No new kafka messages, waited 5 secs")
					continue
				}
				//topicMsg := <-pc.Messages()
				zap.S().Debug("Consumer ", k.topicName, ": Consumed message key=", string(topicMsg.Key))

				// Broadcast
				k.broadcaster.ConsumerChan <- topicMsg

				zap.S().Debug("Consumer ", k.topicName, ": Broadcasted message key=", string(topicMsg.Key))
			}
		}(pc)
	}
	// Waiting, so that client remains alive
	ch := make(chan int, 1)
	<-ch
}
