package consumer

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/mtaimoor1/kafka-logger/writers"
)

type KafkaConsumerGroup struct {
	cg     sarama.ConsumerGroup
	topics []string
}

type exampleConsumerGroupHandler struct {
	writer writers.Writer
}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		fmt.Printf("Content: %s\n", string(msg.Value))
		sess.MarkMessage(msg, "")
	}
	sess.Commit()
	return nil
}

func consumerGroupConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_7_0_0 // specify appropriate version
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = false
	return config
}

func NewConsumerGroup(kafkaUrl []string, groupId string, tps []string) *KafkaConsumerGroup {
	cfg := consumerGroupConfig()
	log.Print("Got the config")
	group, err := sarama.NewConsumerGroup(kafkaUrl, groupId, cfg)
	log.Print("Group object returned")
	if err != nil {
		panic(err)
	}

	return &KafkaConsumerGroup{
		cg:     group,
		topics: tps,
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
	handler := exampleConsumerGroupHandler{}
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		err := k.cg.Consume(ctx, k.topics, handler)
		if err != nil {
			panic(err)
		}
	}
}
