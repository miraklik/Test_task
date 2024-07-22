package main

import (
	"context"
	"log"

	"github.com/IBM/sarama"
)

var producer sarama.SyncProducer

func init() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	var err error

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal("Failed to start kafka: ", err)
	}

	log.Println(producer)
}

func sendTokafka(msg Message) {
	ctx := context.Background()
	message := &sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.StringEncoder(msg.Content),
	}

	_, _, err := producer.SendMessage(message)
	if err != nil {
		log.Println("failed to send to message in Kafka: ", err)
		return
	}

	_, err = conn.Exec(ctx, "UPDATE messages SET processed = TRUE WHERE id = $1", msg.ID)
	if err != nil {
		log.Println("failed to update message status:", err)
	}
}
