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

	consumerTopicNameBlocks := config.Config.ConsumerTopicBlocks
	consumerTopicNameTransactions := config.Config.ConsumerTopicTransactions
	consumerTopicNameLogs := config.Config.ConsumerTopicLogs

	startKafkaTopicConsumers(consumerTopicNameBlocks)
	startKafkaTopicConsumers(consumerTopicNameTransactions)
	startKafkaTopicConsumers(consumerTopicNameLogs)
}

func startKafkaTopicConsumers(topicName string) {
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

	zap.S().Info("kafkaBroker=", kafkaBroker, " consumerTopics=", topicName, " consumerGroup=", consumerGroup, " - Starting Consumers")

	// Start from last read message
	go KafkaTopicConsumers[topicName].consumeGroup(consumerGroup+"-HEAD", sarama.OffsetOldest)

	// Start from 0 always
	go KafkaTopicConsumers[topicName].consumeGroup(consumerGroup+"-TAIL", 0)
}

func (k *kafkaTopicConsumer) consumeGroup(group string, startOffset int64) {
	version, err := sarama.ParseKafkaVersion("2.1.1")
	if err != nil {
		zap.S().Panic("CONSUME GROUP ERROR: parsing Kafka version: ", err.Error())
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = version
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	if startOffset != 0 {
		saramaConfig.Consumer.Offsets.Initial = startOffset
	} else {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

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
		claimConsumer := &ClaimConsumer{
			startOffset: startOffset,
			topicName:   k.topicName,
			topicChan:   k.TopicChan,
			group:       group,
		}

		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			err := consumerGroup.Consume(ctx, []string{k.topicName}, claimConsumer)
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

type ClaimConsumer struct {
	startOffset int64
	topicName   string
	topicChan   chan *sarama.ConsumerMessage
	group       string
}

func (*ClaimConsumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (*ClaimConsumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (c *ClaimConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for {
		var topicMsg *sarama.ConsumerMessage
		select {
		case msg := <-claim.Messages():
			topicMsg = msg
		case <-time.After(5 * time.Second):
			zap.S().Info("GROUP=", c.group, ",TOPIC=", c.topicName, " - No new kafka messages, waited 5 secs...")
			continue
		}

		zap.S().Debug("GROUP=", c.group, ",TOPIC=", c.topicName, ",OFFSET=", topicMsg.Offset, " - New message")

		// Commit offset
		if c.startOffset != 0 {
			// If startOffset is 0, this is the TAIL consumer
			// Only head consumer commits offsets
			zap.S().Debug("GROUP=", c.group, ",TOPIC=", c.topicName, ",OFFSET=", topicMsg.Offset, " - Committing offset")
			sess.MarkMessage(topicMsg, "")
		}

		// Broadcast
		c.topicChan <- topicMsg
	}
	return nil
}
