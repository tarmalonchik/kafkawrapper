package kafka

import (
	"time"
)

const (
	timeout = 10 * time.Second
	network = "tcp"
)

type Config struct {
	KafkaPassword           string `envconfig:"KAFKA_PASSWORD" required:"true"`
	KafkaUser               string `envconfig:"KAFKA_USER" required:"true"`
	KafkaPort               string `envconfig:"KAFKA_PORT" required:"true"`
	KafkaControllersCount   int    `envconfig:"KAFKA_CONTROLLERS_COUNT" required:"true"`
	KafkaHeadlessServiceURL string `envconfig:"KAFKA_HEADLESS_SERVICE_URL" required:"true"`
	KafkaBrokerURLTemplate  string `envconfig:"KAFKA_BROKER_URL_TEMPLATE" required:"true"`
}
