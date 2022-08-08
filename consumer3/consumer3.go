package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

const (
	topic = "example-123"
	consumerID_1 = "cid1"
	consumerID_2 = "cid2"
	brokerAddress1 = "localhost:9092"
	brokerAddress2 = "localhost:9094"
)

func main(){

	ctx := context.Background()
	// initialize a new Reader to read the messages from the subscribed kafka topic
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress1, brokerAddress2},
		Topic: topic,
		GroupID: consumerID_2,
	})

	// Listen to kafka server for the new messages
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			break
		}

		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}