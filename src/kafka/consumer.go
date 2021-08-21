package kafka

import (
	"context"
	"time"

	"go.uber.org/zap"
	"gopkg.in/Shopify/sarama.v1"

	"github.com/geometry-labs/icon-blocks/config"
)

type kafkaTopicConsumer struct {
	brokerURL string
	topicName string
	TopicChan chan *sarama.ConsumerMessage
}

var KafkaTopicConsumers map[string]*kafkaTopicConsumer

// StartWorkerConsumers - start consumer goroutines for Worker config
func StartWorkerConsumers() {
	kafkaBroker := config.Config.KafkaBrokerURL
	consumerTopics := config.Config.ConsumerTopics
	consumerGroup := config.Config.ConsumerGroup

	// Init KafkaBrokerURL
	KafkaTopicConsumers = make(map[string]*kafkaTopicConsumer)

	for _, t := range consumerTopics {

		KafkaTopicConsumers[t] = &kafkaTopicConsumer{
			kafkaBroker,
			t,
			make(chan *sarama.ConsumerMessage),
		}

		// One routine per topic
		go KafkaTopicConsumers[t].consumeGroup(consumerGroup)

		zap.S().Info("Start Consumer: kafkaBroker=", kafkaBroker, " consumerTopics=", t, " consumerGroup=", consumerGroup)
	}
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
		k.TopicChan <- topicMsg

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
