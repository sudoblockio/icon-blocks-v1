package redis

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-blocks/config"
)

func (c *Client) Publish(data []byte) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Publish
		err := c.client.Publish(ctx, config.Config.RedisChannel, string(data)).Err()
		if err != nil {
			// Failure
			zap.S().Warn("Redis Publish: Cannot publish message...retrying in 3 second")
			time.Sleep(3 * time.Second)

			continue
		}

		// Success
		break
	}
}

func (c *Client) StartSubscriber() {

	go func() {
		subscriberChannel := c.pubsub.Channel()
		inputChannel := GetBroadcaster().InputChannel

		for {
			redisMsg := <-subscriberChannel

			inputChannel <- []byte(redisMsg.Payload)
		}
	}()
}
