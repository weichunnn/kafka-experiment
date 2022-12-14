package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic          = "example-topic"
	broker1Address = "localhost:9093"
	broker2Address = "localhost:9094"
	broker3Address = "localhost:9095"
)

func produce(ctx context.Context) {
	i := 0
	logger := log.New(os.Stdout, "kafka writer: ", 0)
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
		Logger:  logger,
		// wait until we get 10 messages before writing
		BatchSize: 10,
		// no matter what happens, write all pending messages
		// every 2 seconds
		BatchTimeout: 2 * time.Second,
		RequiredAcks: 1, // only need leader to respond OK
	})

	defer w.Close()

	for {
		err :=
			w.WriteMessages(ctx, kafka.Message{
				Key:   []byte(strconv.Itoa(i)),
				Value: []byte("this is a sample message " + strconv.Itoa(i)),
			})

		if err != nil {
			panic("cannot write message with error: " + err.Error())
		}

		fmt.Println("wrote: ", i)
		i++
		time.Sleep(time.Second)
	}
}

func consume(ctx context.Context) {
	readLogger := log.New(os.Stdout, "kafka reader: ", 0)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker1Address, broker2Address, broker3Address},
		Topic:    topic,
		GroupID:  "consumer-group-1",
		Logger:   readLogger,
		MinBytes: 5,
		MaxBytes: 1e6,
		// wait for at most 3 seconds before receiving new data
		MaxWait: 3 * time.Second,
	})
	defer r.Close()

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("cannot read message with error: " + err.Error())
		}

		fmt.Printf("received %s from partition %d \n", string(msg.Value), msg.Partition)
	}
}

func main() {
	ctx := context.Background()

	go produce(ctx)
	consume(ctx)
}
