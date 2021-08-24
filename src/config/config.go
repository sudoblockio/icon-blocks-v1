package config

import (
	"log"

	"github.com/kelseyhightower/envconfig"
)

type configType struct {
	Name        string `envconfig:"NAME" required:"false" default:"blocks-service"`
	NetworkName string `envconfig:"NETWORK_NAME" required:"false" default:"mainnnet"`

	// Ports
	Port        string `envconfig:"PORT" required:"false" default:"8000"`
	HealthPort  string `envconfig:"HEALTH_PORT" required:"false" default:"8180"`
	MetricsPort string `envconfig:"METRICS_PORT" required:"false" default:"9400"`

	// Prefix
	RestPrefix      string `envconfig:"REST_PREFIX" required:"false" default:"/api/v1"`
	WebsocketPrefix string `envconfig:"WEBSOCKET_PREFIX" required:"false" default:"/ws/v1"`
	HealthPrefix    string `envconfig:"HEALTH_PREFIX" required:"false" default:"/health"`
	MetricsPrefix   string `envconfig:"METRICS_PREFIX" required:"false" default:"/metrics"`

	// Monitoring
	HealthPollingInterval int `envconfig:"HEALTH_POLLING_INTERVAL" required:"false" default:"10"`

	// Logging
	LogLevel    string `envconfig:"LOG_LEVEL" required:"false" default:"INFO"`
	LogToFile   bool   `envconfig:"LOG_TO_FILE" required:"false" default:"false"`
	LogFileName string `envconfig:"LOG_FILE_NAME" required:"false" default:"blocks-service.log"`
	LogFormat   string `envconfig:"LOG_FORMAT" required:"false" default:"json"`

	// Kafka
	KafkaBrokerURL    string `envconfig:"KAFKA_BROKER_URL" required:"false" default:"kafka:9092"`
	SchemaRegistryURL string `envconfig:"SCHEMA_REGISTRY_URL" required:"false" default:"schemaregistry:8081"`
	KafkaGroupID      string `envconfig:"KAFKA_GROUP_ID" required:"false" default:"blocks-service"`

	// Topics
	ConsumerGroup                 string            `envconfig:"CONSUMER_GROUP" required:"false" default:"blocks-consumer-group"`
	ConsumerTopicNameBlocks       string            `envconfig:"CONSUMER_TOPIC_NAME_BLOCKS" required:"false" default:"blocks"`
	ConsumerTopicNameTransactions string            `envconfig:"CONSUMER_TOPIC_NAME_TRANSACTIONS" required:"false" default:"transactions"`
	ConsumerTopicNameLogs         string            `envconfig:"CONSUMER_TOPIC_NAME_LOGS" required:"false" default:"logs"`
	ProducerTopics                []string          `envconfig:"CONSUMER_TOPIC_NAME_LOGS" required:"false" default:""`
	SchemaNameTopics              map[string]string `envconfig:"SCHEMA_NAME_TOPICS" required:"false" default:"blocks-ws:block"`
	SchemaFolderPath              string            `envconfig:"SCHEMA_FOLDER_PATH" required:"false" default:"/app/schemas/"`

	// DB
	DbDriver   string `envconfig:"DB_DRIVER" required:"false" default:"postgres"`
	DbHost     string `envconfig:"DB_HOST" required:"false" default:"localhost"`
	DbPort     string `envconfig:"DB_PORT" required:"false" default:"5432"`
	DbUser     string `envconfig:"DB_USER" required:"false" default:"postgres"`
	DbPassword string `envconfig:"DB_PASSWORD" required:"false" default:"changeme"`
	DbName     string `envconfig:"DB_DBNAME" required:"false" default:"postgres"`
	DbSslmode  string `envconfig:"DB_SSL_MODE" required:"false" default:"disable"`
	DbTimezone string `envconfig:"DB_TIMEZONE" required:"false" default:"UTC"`

	// Endpoints
	MaxPageSize int `envconfig:"MAX_PAGE_SIZE" required:"false" default:"100"`

	// Redis
	RedisHost                     string `envconfig:"REDIS_HOST" required:"false" default:"redis"`
	RedisPort                     string `envconfig:"REDIS_PORT" required:"false" default:"6380"`
	RedisPassword                 string `envconfig:"REDIS_PASSWORD" required:"false" default:""`
	RedisChannel                  string `envconfig:"REDIS_CHANNEL" required:"false" default:"blocks"`
	RedisSentinelClientMode       bool   `envconfig:"REDIS_SENTINEL_CLIENT_MODE" required:"false" default:"false"`
	RedisSentinelClientMasterName string `envconfig:"REDIS_SENTINEL_CLIENT_MASTER_NAME" required:"false" default:"master"`

	// GORM
	GormSilentLogging bool `envconfig:"GORM_SILENT_LOGGING" required:"false" default:"false"`
}

// Config - runtime config struct
var Config configType

// ReadEnvironment - Read and store runtime config
func ReadEnvironment() {
	err := envconfig.Process("", &Config)
	if err != nil {
		log.Fatalf("ERROR: envconfig - %s\n", err.Error())
	}
}
