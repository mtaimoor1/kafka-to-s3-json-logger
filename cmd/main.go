package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/mtaimoor1/kafka-logger/config"
	"github.com/mtaimoor1/kafka-logger/consumer"
)

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error Loading env file: %w", err)
	}
	cfg := config.NewLoggerConfig(os.Getenv("CONFIG_PATH"))

	err = cfg.LoadConfig()
	if err != nil {
		log.Fatal(err)
	}

	topic, err := cfg.GetConfig("topic", "")
	if err != nil {
		log.Print(err)
	}

	// kafka_consumer := consumer.NewKafkaConsumer(topic)
	kafkaUrl, err := cfg.GetConfig("brokers", "localhost:29092")
	if err != nil {
		log.Print(err)
	}
	log.Printf("Creating consumer group for %s on broker %s.", topic, kafkaUrl)
	consumer_group := consumer.NewConsumerGroup(strings.Split(kafkaUrl, ","), fmt.Sprintf("go_%s", topic), strings.Split(topic, ","))
	log.Print("Consumer group created!")

	consumer_group.Start()

}
