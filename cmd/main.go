package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "go.kafkamulticloud/pkg"
)

func main() {
    config, err := multicloud.LoadConfig()
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

    client, err := multicloud.NewMultiCloudKafkaClient(config)
    if err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    sigs := make(chan os.Signal, 1)
    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-sigs
        log.Println("Received shutdown signal")
        cancel()
    }()

    go func() {
        ticker := time.NewTicker(5 * time.Second)
        defer ticker.Stop()
        log.Println("Producer goroutine started")
        for {
            select {
            case <-ctx.Done():
                log.Println("Producer stopped")
                return
            case t := <-ticker.C:
                message := []byte(fmt.Sprintf("Test message at %s", t.Format(time.RFC3339)))
                if err := client.Produce(ctx, message); err != nil {
                    log.Printf("Produce error: %v", err)
                }
            }
        }
    }()

    if err := client.Consume(ctx, func(data []byte) error {
        log.Printf("Consumed message: %s", string(data))
        return nil
    }); err != nil {
        log.Printf("Consume error: %v", err)
    }
    log.Println("Main loop exited")
}