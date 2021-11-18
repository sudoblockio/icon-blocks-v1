package kafka

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/models"
)

type kafkaTopicConsumer struct {
	brokerURL     string
	topicNames    []string
	TopicChannels map[string]chan *sarama.ConsumerMessage
}

var KafkaTopicConsumer *kafkaTopicConsumer

// StartWorkerConsumers - start consumer goroutines for Worker config
func StartWorkerConsumers() {

	// Init topic names
	topicNames := []string{
		config.Config.ConsumerTopicBlocks,
		config.Config.ConsumerTopicTransactions,
		config.Config.ConsumerTopicLogs,
	}

	// Init topic channels
	topicChannels := make(map[string]chan *sarama.ConsumerMessage)
	for _, topicName := range topicNames {
		topicChannels[topicName] = make(chan *sarama.ConsumerMessage)
	}

	// Init consumer
	KafkaTopicConsumer = &kafkaTopicConsumer{
		brokerURL:     config.Config.KafkaBrokerURL,
		topicNames:    topicNames,
		TopicChannels: topicChannels,
	}

	////////////////////
	// Consumer Modes //
	////////////////////

	// Partition
	if config.Config.ConsumerIsPartitionConsumer == true {
		zap.S().Info(
			"kafkaBroker=", config.Config.KafkaBrokerURL,
			" ConsumerPartitionTopic=", config.Config.ConsumerPartitionTopic,
			" ConsumerPartition=", config.Config.ConsumerPartition,
			" ConsumerPartitionStartOffset=", config.Config.ConsumerPartitionStartOffset,
			" - Starting Consumers")
		go KafkaTopicConsumer.consumePartition(
			config.Config.ConsumerPartitionTopic,
			config.Config.ConsumerPartition,
			config.Config.ConsumerPartitionStartOffset,
		)
		return
	}

	// Tail
	if config.Config.ConsumerIsTail == true {
		zap.S().Info(
			"kafkaBroker=", config.Config.KafkaBrokerURL,
			" consumerTopics=", topicNames,
			" consumerGroup=", config.Config.ConsumerGroup+"-"+config.Config.ConsumerJobID,
			" - Starting Consumers")
		go KafkaTopicConsumer.consumeGroup(config.Config.ConsumerGroup + "-" + config.Config.ConsumerJobID)
		return
	}

	// Head
	// Default
	zap.S().Info(
		"kafkaBroker=", config.Config.KafkaBrokerURL,
		" consumerTopics=", topicNames,
		" consumerGroup=", config.Config.ConsumerGroup+"-head",
		" - Starting Consumers")
	go KafkaTopicConsumer.consumeGroup(config.Config.ConsumerGroup + "-head")
	return
}

////////////////////
// Group Consumer //
////////////////////
func (k *kafkaTopicConsumer) consumeGroup(group string) {
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

	// Initial Offset
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

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

	// Get Kafka Jobs from database
	jobID := config.Config.ConsumerJobID
	kafkaJobs := &[]models.KafkaJob{}
	if jobID != "" {
		for {
			// Wait until jobs are present

			kafkaJobs, err = crud.GetKafkaJobModel().SelectMany(
				jobID,
				group,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				zap.S().Info(
					"JobID=", jobID,
					",ConsumerGroup=", group,
					" - Waiting for Kafka Job in database...")
				time.Sleep(1)
				continue
			} else if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}

			break
		}
	}

	// From example: /sarama/blob/master/examples/consumergroup/main.go
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		claimConsumer := &ClaimConsumer{
			topicNames: k.topicNames,
			topicChans: k.TopicChannels,
			group:      group,
			kafkaJobs:  *kafkaJobs,
		}

		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			err := consumerGroup.Consume(ctx, k.topicNames, claimConsumer)
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
	topicNames []string
	topicChans map[string]chan *sarama.ConsumerMessage
	group      string
	kafkaJobs  []models.KafkaJob
}

func (c *ClaimConsumer) Setup(_ sarama.ConsumerGroupSession) error { return nil }
func (*ClaimConsumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (c *ClaimConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	topicName := claim.Topic()
	partition := uint64(claim.Partition())

	// find kafka job
	var kafkaJob *models.KafkaJob = nil
	for i, k := range c.kafkaJobs {
		if k.Partition == partition && k.Topic == topicName {
			kafkaJob = &(c.kafkaJobs[i])
			break
		}
	}

	for {
		var topicMsg *sarama.ConsumerMessage
		select {
		case msg := <-claim.Messages():
			if msg == nil {
				zap.S().Warn("GROUP=", c.group, ",TOPIC=", topicName, " - Kafka message is nil, exiting ConsumeClaim loop...")
				return nil
			}

			topicMsg = msg
		case <-time.After(5 * time.Second):
			zap.S().Info("GROUP=", c.group, ",TOPIC=", topicName, " - No new kafka messages, waited 5 secs...")
			continue
		case <-sess.Context().Done():
			zap.S().Warn("GROUP=", c.group, ",TOPIC=", topicName, " - Session is done, exiting ConsumeClaim loop...")
			return nil
		}

		zap.S().Info("GROUP=", c.group, ",TOPIC=", topicName, ",PARTITION=", partition, ",OFFSET=", topicMsg.Offset, " - New message")
		sess.MarkMessage(topicMsg, "")

		// Broadcast
		c.topicChans[topicName] <- topicMsg

		// Check if kafka job is done
		// NOTE only applicable if ConsumerKafkaJobID is given
		if kafkaJob != nil &&
			uint64(topicMsg.Offset) >= kafkaJob.StopOffset+1000 {
			// Job done
			zap.S().Info(
				"JOBID=", config.Config.ConsumerJobID,
				"GROUP=", c.group,
				",TOPIC=", topicName,
				",PARTITION=", partition,
				" - Kafka Job done...exiting",
			)
			os.Exit(0)
		}
	}
	return nil
}

////////////////////////
// Partition Consumer //
////////////////////////
func (k *kafkaTopicConsumer) consumePartition(topic string, partition int, startOffset int) {
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

	// Initial Offset
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

	var consumer sarama.Consumer
	for {
		consumer, err = sarama.NewConsumer([]string{k.brokerURL}, saramaConfig)
		if err != nil {
			zap.S().Warn("Creating consumer err: ", err.Error())
			zap.S().Info("Retrying in 3 seconds...")
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}

	// Clean up
	defer func() {
		if err := consumer.Close(); err != nil {
			zap.S().Panic("KAFKA CONSUMER CLOSE PANIC: ", err.Error())
		}
	}()

	// Connect to partition
	pc, err := consumer.ConsumePartition(topic, int32(partition), int64(startOffset))

	if err != nil {
		zap.S().Panic("KAFKA CONSUMER PARTITIONS PANIC: ", err.Error())
	}
	if pc == nil {
		zap.S().Panic("KAFKA CONSUMER PARTITIONS PANIC: Failed to create PartitionConsumer")
	}

	// Read partition
	for {
		var topic_msg *sarama.ConsumerMessage
		select {
		case msg := <-pc.Messages():
			topic_msg = msg
		case consumerErr := <-pc.Errors():
			zap.S().Warn("KAFKA PARTITION CONSUMER ERROR:", consumerErr.Err)
			//consumerErr.Err
			continue
		case <-time.After(5 * time.Second):
			zap.S().Debug("Consumer ", topic, ": No new kafka messages, waited 5 secs")
			continue
		}
		zap.S().Debug("Consumer ", topic, ": Consumed message key=", string(topic_msg.Key))

		// Broadcast
		k.TopicChannels[topic] <- topic_msg

		zap.S().Debug("Consumer ", topic, ": Broadcasted message key=", string(topic_msg.Key))
	}
}
