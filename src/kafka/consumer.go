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
	consumerGroup := config.Config.ConsumerGroup
	consumerIsTail := config.Config.ConsumerIsTail

	if KafkaTopicConsumers == nil {
		KafkaTopicConsumers = make(map[string]*kafkaTopicConsumer)
	}

	KafkaTopicConsumers[topicName] = &kafkaTopicConsumer{
		kafkaBroker,
		topicName,
		make(chan *sarama.ConsumerMessage),
	}

	if consumerIsTail == false {
		// Head
		zap.S().Info(
			"kafkaBroker=", kafkaBroker,
			" consumerTopics=", topicName,
			" consumerGroup=", consumerGroup,
			" - Starting Consumers")
		go KafkaTopicConsumers[topicName].consumeGroup(consumerGroup + "-head")
	} else {
		// Tail
		zap.S().Info("kafkaBroker=", kafkaBroker,
			" consumerTopics=", topicName,
			" consumerGroup=", consumerGroup,
			" - Starting Consumers")
		go KafkaTopicConsumers[topicName].consumeGroup(consumerGroup + "-" + config.Config.ConsumerJobID)
	}
}

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
				config.Config.ConsumerGroup,
				k.topicName,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				zap.S().Info(
					"JobID=", jobID,
					",ConsumerGroup=", config.Config.ConsumerGroup,
					",Topic=", k.topicName,
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
			topicName: k.topicName,
			topicChan: k.TopicChannel,
			group:     group,
			kafkaJobs: *kafkaJobs,
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
	topicName string
	topicChan chan *sarama.ConsumerMessage
	group     string
	kafkaJobs []models.KafkaJob
}

func (c *ClaimConsumer) Setup(_ sarama.ConsumerGroupSession) error { return nil }
func (*ClaimConsumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (c *ClaimConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// find kafka job
	var kafkaJob *models.KafkaJob = nil
	for i, k := range c.kafkaJobs {
		if k.Partition == uint64(claim.Partition()) {
			kafkaJob = &(c.kafkaJobs[i])
			break
		}
	}

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
		case <-sess.Context().Done():
			zap.S().Warn("GROUP=", c.group, ",TOPIC=", c.topicName, " - Session is done, exiting ConsumeClaim loop...")
			return nil
		}

		zap.S().Info("GROUP=", c.group, ",TOPIC=", c.topicName, ",PARTITION=", topicMsg.Partition, ",OFFSET=", topicMsg.Offset, " - New message")
		sess.MarkMessage(topicMsg, "")

		// Broadcast
		c.topicChan <- topicMsg

		// Check if kafka job is done
		// NOTE only applicable if ConsumerKafkaJobID is given
		if kafkaJob != nil &&
			uint64(topicMsg.Offset) >= kafkaJob.StopOffset+1000 {
			// Job done
			zap.S().Info(
				"JOBID=", config.Config.ConsumerJobID,
				"GROUP=", c.group,
				",TOPIC=", c.topicName,
				",PARTITION=", topicMsg.Partition,
				" - Kafka Job done...exiting",
			)
			os.Exit(0)
		}
	}
	return nil
}
