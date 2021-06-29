package kafka

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "time"

    "github.com/segmentio/kafka-go"

    "github.com/samuskitchen/go-kafka-example/pkg"
)

type consumer struct {
    reader *kafka.Reader
}

func NewConsumer(brokers []string, topic string) pkg.Consumer {

    c := kafka.ReaderConfig{
        Brokers:         brokers,
        Topic:           topic,
        MinBytes:        10e3,            // 10KB
        MaxBytes:        10e6,            // 10MB
        MaxWait:         1 * time.Second, // Maximum amount of time to wait for new data to come when fetching batches of messages from kafka.
        ReadLagInterval: -1,
        GroupID:         pkg.Ulid(),
        StartOffset:     kafka.LastOffset,
    }

    return &consumer{kafka.NewReader(c)}
}

func (c *consumer) Read(ctx context.Context, chMsg chan pkg.Message, chErr chan error) {
    defer c.reader.Close()

    for {

        m, err := c.reader.ReadMessage(ctx)
        if err != nil {
            chErr <- errors.New(fmt.Sprintf("error while reading a message: %v", err))
            continue
        }

        var message pkg.Message
        err = json.Unmarshal(m.Value, &message)
        if err != nil {
            chErr <- err
        }

        chMsg <- message
    }
}