package kafka

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/spf13/pflag"
)

var (
	brokersArg string
)

func RegisterFlags(prefix string, flags *pflag.FlagSet) {
	flags.StringVar(&brokersArg, prefix+"brokers", "localhost:9092", "Kafka brokers, comma-separated")
}

func DefaultConfig() *sarama.Config {
	return sarama.NewConfig()
}

func NewClient(config *sarama.Config) (sarama.Client, error) {
	return sarama.NewClient(brokers(), config)
}

func NewConsumerGroup(group string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(brokers(), group, config)
}

func brokers() []string {
	return strings.Split(brokersArg, ",")
}
