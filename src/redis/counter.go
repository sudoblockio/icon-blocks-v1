package redis

import (
	"context"
	"strconv"

	"github.com/go-redis/redis"
)

func (c *Client) GetCount(countKey string) (int64, error) {

	count := int64(0)

	countStr, err := c.client.Get(context.Background(), countKey).Result()
	if err == redis.Nil || countStr == "" {
		countStr = "-1"
		err = nil
	}
	count, err = strconv.ParseInt(countStr, 10, 64)

	return count, err
}

func (c *Client) SetCount(countKey string, count int64) error {

	err := c.client.Set(context.Background(), countKey, count, 0).Err()

	return err
}

func (c *Client) IncCount(countKey string) (int64, error) {

	count, err := c.client.Incr(context.Background(), countKey).Result()

	return count, err
}
