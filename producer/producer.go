package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)
const (
	topic = "example-123"
	brokerAddress1 = "localhost:9092"
	brokerAddress2 = "localhost:9094"
)

func main(){

	ctx := context.Background()
	// we initialize a counter with the broker addresses
	w := &kafka.Writer{
		Addr: kafka.TCP(brokerAddress1),
		Topic: topic,
		RequiredAcks: kafka.RequireAll,
		AllowAutoTopicCreation: true,
		Async: true,
		Completion: func(messages []kafka.Message, err error) {

			if err != nil {
				fmt.Println(err)
				return
			}

			for _, val := range messages {
				fmt.Printf("messages sent, offset %d, key %s, val %s \n", val.Offset, val.Key, val.Value)
			}
		},
	}

	// Using this writer we write into the kafka topics
	for {
		err := w.WriteMessages(ctx, 
			kafka.Message{
				Key: []byte("Key"),
				Value: []byte("Value"),
			},
		)

		// Check if the message was written into the topic
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}

		time.Sleep(1*time.Second)
	}
	
}