package main

import (
	"log"
	"os"

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
		log.Fatal("Topics not found in the config file")
	}

	kafka_consumer := consumer.NewKafkaConsumer(topic)

	if err != nil {
		log.Fatalf("Unable to create consumer: %v", err)
	}

	go kafka_consumer.Start()

}
