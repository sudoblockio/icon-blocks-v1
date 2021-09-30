package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-blocks/config"
)

type kafkaTopicConsumer struct {
	brokerURL    string
	topicName    string
	TopicChannel chan *sarama.ConsumerMessage
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
	consumerGroupHead := config.Config.ConsumerGroupHead
	consumerGroupTail := config.Config.ConsumerGroupTail

	if KafkaTopicConsumers == nil {
		KafkaTopicConsumers = make(map[string]*kafkaTopicConsumer)
	}

	KafkaTopicConsumers[topicName] = &kafkaTopicConsumer{
		kafkaBroker,
		topicName,
		make(chan *sarama.ConsumerMessage),
	}

	zap.S().Info("kafkaBroker=", kafkaBroker, " consumerTopics=", topicName, " consumerGroup=", consumerGroup, " - Starting Consumers")

	go KafkaTopicConsumers[topicName].consumeGroup(consumerGroupHead, sarama.OffsetOldest)
	go KafkaTopicConsumers[topicName].consumeGroup(consumerGroupTail, sarama.OffsetOldest)
}

func (k *kafkaTopicConsumer) consumeGroup(group string, startOffset int64) {
	version, err := sarama.ParseKafkaVersion("2.1.1")
	if err != nil {
		zap.S().Panic("CONSUME GROUP ERROR: parsing Kafka version: ", err.Error())
	}

	///////////////////////////
	// Consumer Group Config //
	///////////////////////////

	saramaConfig := sarama.NewConfig()

	// Version
	saramaConfig.Version = version

	// Balance Strategy
	switch config.Config.ConsumerGroupBalanceStrategy {
	case "BalanceStrategyRange":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	case "BalanceStrategySticky":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "BalanceStrategyRoundRobin":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	default:
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	}

	// Start offset
	if startOffset != 0 {
		saramaConfig.Consumer.Offsets.Initial = startOffset
	} else {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	var consumerGroup sarama.ConsumerGroup
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
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		claimConsumer := &ClaimConsumer{
			startOffset: startOffset,
			topicName:   k.topicName,
			topicChan:   k.TopicChannel,
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

func (c *ClaimConsumer) Setup(sess sarama.ConsumerGroupSession) error {

	/*
		// Reset offsets
		if c.startOffset == 0 {
			partitions := sess.Claims()[c.topicName]

			for _, p := range partitions {
				sess.ResetOffset(c.topicName, p, 0, "reset")
			}
		}
	*/

	return nil
}
func (*ClaimConsumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (c *ClaimConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for {
		var topicMsg *sarama.ConsumerMessage
		select {
		case msg := <-claim.Messages():
			if msg == nil {
				zap.S().Warn("GROUP=", c.group, ",TOPIC=", c.topicName, " - Kafka message is nil, exiting ConsumeClaim loop...")
				return nil
			}
			topicMsg = msg
		case <-time.After(5 * time.Second):
			zap.S().Info("GROUP=", c.group, ",TOPIC=", c.topicName, " - No new kafka messages, waited 5 secs...")
			continue
		}

		zap.S().Info("GROUP=", c.group, ",TOPIC=", c.topicName, ",PARTITION=", topicMsg.Partition, ",OFFSET=", topicMsg.Offset, " - New message")
		sess.MarkMessage(topicMsg, "")

		// Broadcast
		c.topicChan <- topicMsg
	}
	return nil
}
