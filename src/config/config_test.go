package config

import (
	"os"

	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnvironment(t *testing.T) {
	assert := assert.New(t)

	// Set env
	envMap := map[string]string{
		"NAME":                    "name",
		"PORT":                    "port",
		"HEALTH_PORT":             "health_port",
		"METRICS_PORT":            "metrics_port",
		"REST_PREFIX":             "rest_prefix",
		"WEBSOCKET_PREFIX":        "websocket_prefix",
		"HEALTH_PREFIX":           "health_prefix",
		"METRICS_PREFIX":          "metrics_prefix",
		"HEALTH_POLLING_INTERVAL": "5",
		"LOG_LEVEL":               "log_level",
		"LOG_TO_FILE":             "true",
		"NETWORK_NAME":            "network_name",
		"KAFKA_BROKER_URL":        "kafka_broker_url",
		"SCHEMA_REGISTRY_URL":     "schema_registry_url",
		"KAFKA_GROUP_ID":          "kafka_group_id",
		"CONSUMER_TOPICS":         "topic_1,topic_2",
		"PRODUCER_TOPICS":         "topic_1,topic_2,topic_3",
		"SCHEMA_NAME_TOPICS":      "schema_1:schema_1,schema_2:schema_2",
	}

	for k, v := range envMap {
		os.Setenv(k, v)
	}

	// Load env
	ReadEnvironment()

	// Check env
	assert.Equal(envMap["NAME"], Config.Name)
	assert.Equal(envMap["PORT"], Config.Port)
	assert.Equal(envMap["HEALTH_PORT"], Config.HealthPort)
	assert.Equal(envMap["METRICS_PORT"], Config.MetricsPort)
	assert.Equal(envMap["REST_PREFIX"], Config.RestPrefix)
	assert.Equal(envMap["WEBSOCKET_PREFIX"], Config.WebsocketPrefix)
	assert.Equal(envMap["HEALTH_PREFIX"], Config.HealthPrefix)
	assert.Equal(envMap["METRICS_PREFIX"], Config.MetricsPrefix)
	assert.Equal(5, Config.HealthPollingInterval)
	assert.Equal(envMap["LOG_LEVEL"], Config.LogLevel)
	assert.Equal(true, Config.LogToFile)
	assert.Equal(envMap["NETWORK_NAME"], Config.NetworkName)
	assert.Equal(envMap["KAFKA_BROKER_URL"], Config.KafkaBrokerURL)
	assert.Equal(envMap["SCHEMA_REGISTRY_URL"], Config.SchemaRegistryURL)
	assert.Equal(envMap["KAFKA_GROUP_ID"], Config.KafkaGroupID)
	assert.Equal(2, len(Config.ConsumerTopics))
	assert.Equal(3, len(Config.ProducerTopics))
	assert.Equal("schema_1", Config.SchemaNameTopics["schema_1"])
	assert.Equal("schema_2", Config.SchemaNameTopics["schema_2"])
}
