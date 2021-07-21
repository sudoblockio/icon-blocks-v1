package kafka

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gopkg.in/Shopify/sarama.v1"

	"github.com/geometry-labs/icon-blocks/config"
)

func StartApiConsumers() {
	kafka_broker := config.Config.KafkaBrokerURL
	consumer_topics := config.Config.ConsumerTopics

	zap.S().Debug("Start Consumer: kafka_broker=", kafka_broker, " consumer_topics=", consumer_topics)

	BroadcastFunc := func(channel chan *sarama.ConsumerMessage, message *sarama.ConsumerMessage) {
		select {
		case channel <- message:
			return
		default:
			return
		}
	}

	for _, t := range consumer_topics {
		// Broadcaster indexed in Broadcasters map
		// Starts go routine
		newBroadcaster(t, BroadcastFunc)

		topic_consumer := &KafkaTopicConsumer{
			kafka_broker,
			t,
			Broadcasters[t],
		}

		// One routine per topic
		zap.S().Debug("Start Consumers: Starting ", t, " consumer...")
		go topic_consumer.consumeTopic()
	}
}

func StartWorkerConsumers() {
	kafka_broker := config.Config.KafkaBrokerURL
	consumer_topics := config.Config.ConsumerTopics
	consumer_group := config.Config.ConsumerGroup

	zap.S().Info("Start Consumer Group: kafka_broker=", kafka_broker, " consumer_topics=", consumer_topics, " consumer_group=", consumer_group)

	for _, t := range consumer_topics {
		// Broadcaster indexed in Broadcasters map
		// Starts go routine
		newBroadcaster(t, nil)

		topic_consumer := &KafkaTopicConsumer{
			kafka_broker,
			t,
			Broadcasters[t],
		}

		// One routine per topic
		zap.S().Debug("Start Consumers: Starting ", t, " consumer...")
		go topic_consumer.consumeGroup(consumer_group)
	}
}

type KafkaTopicConsumer struct {
	BrokerURL   string
	TopicName   string
	Broadcaster *TopicBroadcaster
}

// Used internally by Sarama
func (*KafkaTopicConsumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (*KafkaTopicConsumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (k *KafkaTopicConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		var topic_msg *sarama.ConsumerMessage
		select {
		case msg := <-claim.Messages():
			topic_msg = msg
		case <-time.After(5 * time.Second):
			zap.S().Debug("Consumer ", k.TopicName, ": No new kafka messages, waited 5 secs")
			continue
		}

		zap.S().Info("New Kafka Consumer Group Message: offset=", topic_msg.Offset, " key=", string(topic_msg.Key))

		// Commit offset
		sess.MarkMessage(topic_msg, "")

		// Broadcast
		k.Broadcaster.ConsumerChan <- topic_msg

		zap.S().Debug("Consumer ", k.TopicName, ": Broadcasted message key=", string(topic_msg.Key))
	}
	return nil
}

func (k *KafkaTopicConsumer) consumeGroup(group string) {
	version, err := sarama.ParseKafkaVersion("2.1.1")
	if err != nil {
		zap.S().Panic("CONSUME GROUP ERROR: parsing Kafka version: ", err.Error())
	}

	sarama_config := sarama.NewConfig()
	sarama_config.Version = version
	sarama_config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	sarama_config.Consumer.Offsets.Initial = sarama.OffsetOldest

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup([]string{k.BrokerURL}, group, sarama_config)
	if err != nil {
		zap.S().Panic("CONSUME GROUP ERROR: creating consumer group client: ", err.Error())
	}

	// From example: /sarama/blob/master/examples/consumergroup/main.go
	go func() {
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			err := client.Consume(ctx, []string{k.TopicName}, k)
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

func (k *KafkaTopicConsumer) consumeTopic() {
	sarama_config := sarama.NewConfig()
	sarama_config.Consumer.Return.Errors = true

	// Connect consumer on Retry
	var consumer sarama.Consumer
	err := backoff.Retry(func() error {
		var err error
		consumer, err = sarama.NewConsumer([]string{k.BrokerURL}, sarama_config)
		if err != nil {
			zap.S().Warn("Kafka New Consumer Error: ", err.Error())
			zap.S().Warn("Cannot conCect to kafka broker retrying...")
			return err
		}

		return nil
	}, backoff.NewExponentialBackOff())

	if err != nil {
		zap.S().Panic("KAFKA CONSUMER NEWCONSUMER PANIC: ", err.Error())
	}

	zap.S().Info("Kakfa Consumer: Broker connection established")
	defer func() {
		if err := consumer.Close(); err != nil {
			zap.S().Warn("KAFKA CONSUMER CLOSE: ", err.Error())
		}
	}()

	offset := sarama.OffsetNewest
	partitions, err := consumer.Partitions(k.TopicName)
	if err != nil {
		zap.S().Panic("KAFKA CONSUMER PARTITIONS PANIC: ", err.Error(), " Topic: ", k.TopicName)
	}

	zap.S().Debug("Consumer ", k.TopicName, ": Started consuming")
	for _, p := range partitions {
		pc, err := consumer.ConsumePartition(k.TopicName, p, offset)
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
				var topic_msg *sarama.ConsumerMessage
				select {
				case msg := <-pc.Messages():
					topic_msg = msg
				case consumerErr := <-pc.Errors():
					zap.S().Warn("KAFKA PARTITION CONSUMER ERROR:", consumerErr.Err)
					continue
				case <-time.After(5 * time.Second):
					zap.S().Debug("Consumer ", k.TopicName, ": No new kafka messages, waited 5 secs")
					continue
				}
				//topic_msg := <-pc.Messages()
				zap.S().Debug("Consumer ", k.TopicName, ": Consumed message key=", string(topic_msg.Key))

				// Broadcast
				k.Broadcaster.ConsumerChan <- topic_msg

				zap.S().Debug("Consumer ", k.TopicName, ": Broadcasted message key=", string(topic_msg.Key))
			}
		}(pc)
	}
	// Waiting, so that client remains alive
	ch := make(chan int, 1)
	<-ch
}
