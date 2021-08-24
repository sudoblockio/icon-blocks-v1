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

	consumerTopicNameBlocks := config.Config.ConsumerTopicNameBlocks
	consumerTopicNameTransactions := config.Config.ConsumerTopicNameTransactions
	consumerTopicNameLogs := config.Config.ConsumerTopicNameLogs

	startKafkaTopicConsumer(consumerTopicNameBlocks)
	startKafkaTopicConsumer(consumerTopicNameTransactions)
	startKafkaTopicConsumer(consumerTopicNameLogs)
}

func startKafkaTopicConsumer(topicName string) {
	kafkaBroker := config.Config.KafkaBrokerURL
	consumerGroup := config.Config.ConsumerGroup

	if KafkaTopicConsumers == nil {
		KafkaTopicConsumers = make(map[string]*kafkaTopicConsumer)
	}

	KafkaTopicConsumers[topicName] = &kafkaTopicConsumer{
		kafkaBroker,
		topicName,
		make(chan *sarama.ConsumerMessage),
	}

	// One routine per topic
	go KafkaTopicConsumers[topicName].consumeGroup(consumerGroup)

	zap.S().Info("Start Consumer: kafkaBroker=", kafkaBroker, " consumerTopics=", topicName, " consumerGroup=", consumerGroup)
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

	var consumerGroup sarama.ConsumerGroup
	ctx, cancel := context.WithCancel(context.Background())
	for {
		consumerGroup, err = sarama.NewConsumerGroup([]string{k.brokerURL}, group, saramaConfig)
		if err != nil {
			zap.S().Warn("Creating consumer group consumerGroup err: ", err.Error())
			zap.S().Info("Retrying in 3 seconds...")
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}

	// From example: /sarama/blob/master/examples/consumergroup/main.go
	go func() {
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			err := consumerGroup.Consume(ctx, []string{k.topicName}, k)
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

	// Waiting, so that consumerGroup remains alive
	ch := make(chan int, 1)
	<-ch
	cancel()
}
