package main

import (
	"context"
	"fmt"
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
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
	})

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
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
		GroupID: "consumer-group-1",
	})

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("cannot read message with error: " + err.Error())
		}

		fmt.Printf("received %s from partion %d \n", string(msg.Value), msg.Partition)
	}
}

func main() {
	ctx := context.Background()

	go produce(ctx)
	consume(ctx)
}
