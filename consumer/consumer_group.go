package consumer

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
)

type KafkaConsumerGroup struct {
	cg     sarama.ConsumerGroup
	topics []string
}

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg, "")
	}
	sess.Commit()
	return nil
}

func consumerGroupConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0 // specify appropriate version
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = false
	return config
}

func NewConsumerGroup(kafkaUrl []string, groupId string, topics []string) *KafkaConsumerGroup {

	group, err := sarama.NewConsumerGroup(kafkaUrl, groupId, consumerGroupConfig())
	if err != nil {
		panic(err)
	}

	return &KafkaConsumerGroup{
		cg: group,
	}
}

func (k KafkaConsumerGroup) Start() {

	defer func() { _ = k.cg.Close() }()

	// Track errors
	go func() {
		for err := range k.cg.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	// Iterate over consumer sessions.
	ctx := context.Background()
	for {
		handler := exampleConsumerGroupHandler{}

		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		err := k.cg.Consume(ctx, k.topics, handler)
		if err != nil {
			panic(err)
		}
	}
}
