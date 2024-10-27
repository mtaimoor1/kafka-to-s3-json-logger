package consumer

import (
	"log"
	"strings"

	"github.com/IBM/sarama"
)

type KafkaConsumer struct {
	consumer sarama.Consumer
	cfg      *sarama.Config
	topic    string
}

func NewKafkaConsumer(topic string) *KafkaConsumer {
	return &KafkaConsumer{
		consumer: nil,
		cfg:      newConsumerConfig(),
		topic:    topic,
	}
}

func newConsumerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = "json-logger"
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	return cfg
}

func (c *KafkaConsumer) getConsumer(broker_ip string) {

	cns, err := sarama.NewConsumer(strings.Split(broker_ip, ","), c.cfg)

	if err != nil {
		log.Fatalf("Unable to create consumer: %v", err)
	}

	c.consumer = cns

}

func (c *KafkaConsumer) Start() {
	for {
		// c.consumer
	}
}
