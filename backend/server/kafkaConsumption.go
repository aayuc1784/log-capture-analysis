package main

import (
    "fmt"
    "context"
    "encoding/json"

    "github.com/segmentio/kafka-go"
)

func KafkaConsumer(ingestionContext *IngestionContext, topics []string){
    for _,topic := range topics {
        go func(topic string){
            reader := kafka.NewReader(kafka.ReaderConfig{
                    Brokers: []string {kafkaBrokerHost},
                    Topic: topic,
                    GroupID: "log-consumer-group",
                    MaxBytes: 10e6,
                },
            )
            defer reader.Close()
            fmt.Println("Kafka Consumer started")
            for {
                message, err := reader.FetchMessage(context.Background())
                if err != nil {
                    fmt.Println("Error in consuming", err)
                    continue
                }
                var log Log
                err = json.Unmarshal(message.Value, &log)
                if err != nil {
                    fmt.Printf("Error decoding log from Kafka message for topic %s: %v\n", topic, err)
                    continue
                }
                fmt.Println(log)
                ingestionContext.logChannel <- log
                reader.CommitMessages(context.Background(), message)
            }
        } (topic)
    }
}