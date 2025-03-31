package multicloud

import (
    "context"
    "crypto/tls"
    "fmt"
    "log"
    "os"
    "strings"
    "time"

    //"github.com/Azure/azure-sdk-for-go/sdk/azcore"
    "github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
    "github.com/Azure/azure-sdk-for-go/sdk/azidentity"
   // awsconfig "github.com/aws/aws-sdk-go-v2/config"
    "github.com/joho/godotenv"
    "github.com/twmb/franz-go/pkg/kgo"
    "github.com/twmb/franz-go/pkg/sasl/oauth"
)

// CloudProvider represents supported cloud providers
type CloudProvider string

const (
    AWS   CloudProvider = "AWS"
    Azure CloudProvider = "AZURE"
)

// EventBrokerConfig holds configuration for event brokers
type EventBrokerConfig struct {
    CloudProvider     CloudProvider
    AWSAccessKey      string
    AWSSecretKey      string
    AWSRegion         string
    KinesisStreamName string // Used as Kafka topic name for AWS proxy or MSK
    AzureNamespace    string // e.g., kafkaeventconn.servicebus.windows.net
    AzureEventHubName string
    AzureTenantID     string
    KafkaTopic        string
}

// EventBroker interface defines the common methods
type EventBroker interface {
    Produce(ctx context.Context, event []byte) error
    Consume(ctx context.Context, handler func([]byte) error) error
    Close() error
}

// kafkaBroker for Kafka protocol (Kinesis proxy, Event Hubs)
type kafkaBroker struct {
    client *kgo.Client
    topic  string
}

// MultiCloudKafkaClient manages the multi-cloud implementation
type MultiCloudKafkaClient struct {
    broker EventBroker
    config EventBrokerConfig
}

// LoadConfig loads configuration from .env file or environment variables
func LoadConfig() (EventBrokerConfig, error) {
    err := godotenv.Load("../.env")
    if err != nil {
        log.Printf("No .env file found or error loading it: %v", err)
    }

    cloudProvider := CloudProvider(strings.ToUpper(os.Getenv("CLOUD_PROVIDER")))
    if cloudProvider != AWS && cloudProvider != Azure {
        return EventBrokerConfig{}, fmt.Errorf("invalid or missing CLOUD_PROVIDER environment variable")
    }

    config := EventBrokerConfig{
        CloudProvider:     cloudProvider,
        AWSAccessKey:      os.Getenv("AWS_ACCESS_KEY"),
        AWSSecretKey:      os.Getenv("AWS_SECRET_KEY"),
        AWSRegion:         os.Getenv("AWS_REGION"),
        KinesisStreamName: os.Getenv("KINESIS_STREAM_NAME"),
        AzureNamespace:    os.Getenv("AZURE_NAMESPACE"),
        AzureEventHubName: os.Getenv("AZURE_EVENTHUB_NAME"),
        AzureTenantID:     os.Getenv("AZURE_TENANT_ID"),
        KafkaTopic:        os.Getenv("KAFKA_TOPIC"),
    }

    log.Printf("Loaded config: %+v", config)
    return config, nil
}

// NewMultiCloudKafkaClient creates a new multi-cloud Kafka client
func NewMultiCloudKafkaClient(cfg EventBrokerConfig) (*MultiCloudKafkaClient, error) {
    broker, err := newKafkaBroker(cfg)
    if err != nil {
        return nil, err
    }

    return &MultiCloudKafkaClient{
        broker: broker,
        config: cfg,
    }, nil
}

// newKafkaBroker creates a new Kafka broker for AWS Kinesis proxy or Azure Event Hubs
func newKafkaBroker(cfg EventBrokerConfig) (EventBroker, error) {
    var opts []kgo.Opt
    var brokerAddr string
    var topic string

    switch cfg.CloudProvider {
    case AWS:
        // AWS Kinesis via Kafka proxy (assumes proxy runs locally)
        if cfg.AWSRegion == "" {
            return nil, fmt.Errorf("AWS_REGION must be specified for AWS provider")
        }
        if cfg.KinesisStreamName == "" {
            return nil, fmt.Errorf("KINESIS_STREAM_NAME must be specified for AWS provider (used as Kafka topic)")
        }

        // Use local proxy address (adjust if proxy runs elsewhere)
        brokerAddr = "localhost:9092"
        opts = []kgo.Opt{
            kgo.SeedBrokers(brokerAddr),
            // No SASL or TLS needed for local proxy; adjust if proxy requires it
            kgo.ConsumerGroup("multi-cloud-group-" + time.Now().Format("20060102150405")),
            kgo.ConsumeTopics(cfg.KinesisStreamName),
            kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
            kgo.AutoCommitInterval(5 * time.Second),
        }
        topic = cfg.KinesisStreamName

    case Azure:
        // Azure Event Hubs with OAuthBearer
        credOptions := &azidentity.DefaultAzureCredentialOptions{
            TenantID: cfg.AzureTenantID,
        }
        log.Printf("Attempting to create DefaultAzureCredential")
        cred, err := azidentity.NewDefaultAzureCredential(credOptions)
        if err != nil {
            log.Printf("Failed to create DefaultAzureCredential: %v", err)
            return nil, fmt.Errorf("failed to create DefaultAzureCredential: %v", err)
        }
        log.Printf("Successfully created DefaultAzureCredential")

        scope := fmt.Sprintf("https://%s", cfg.AzureNamespace)
        log.Printf("Requesting token with scope: %s", scope)
        ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
        defer cancel()
        token, err := cred.GetToken(ctx, policy.TokenRequestOptions{
            Scopes: []string{scope},
        })
        if err != nil {
            log.Printf("Failed to get initial token: %v", err)
            return nil, fmt.Errorf("failed to get initial token: %v", err)
        }
        log.Printf("Initial token obtained: expires at %v, token: %s", token.ExpiresOn, token.Token[:50]+"...")

        brokerAddr = fmt.Sprintf("%s:9093", cfg.AzureNamespace)
        opts = []kgo.Opt{
            kgo.SeedBrokers(brokerAddr),
            kgo.DialTLSConfig(&tls.Config{
                MinVersion: tls.VersionTLS12,
            }),
            kgo.SASL(oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
                token, err := cred.GetToken(ctx, policy.TokenRequestOptions{
                    Scopes: []string{scope},
                })
                if err != nil {
                    log.Printf("Failed to refresh token: %v", err)
                    return oauth.Auth{}, err
                }
                if time.Until(token.ExpiresOn) <= 5*time.Minute {
                    log.Printf("Token refreshed successfully, expires at: %v", token.ExpiresOn)
                }
                return oauth.Auth{
                    Token:      token.Token,
                    Extensions: map[string]string{},
                }, nil
            })),
            kgo.ConsumerGroup("multi-cloud-group-" + time.Now().Format("20060102150405")),
            kgo.ConsumeTopics(cfg.AzureEventHubName),
            kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
            kgo.AutoCommitInterval(5 * time.Second),
        }
        topic = cfg.AzureEventHubName

    default:
        return nil, fmt.Errorf("unsupported cloud provider: %s", cfg.CloudProvider)
    }

    client, err := kgo.NewClient(opts...)
    if err != nil {
        log.Printf("Failed to create Kafka client: %v", err)
        return nil, fmt.Errorf("failed to create Kafka client: %v", err)
    }
    log.Printf("Kafka client created successfully")

    // Verify connection by pinging brokers
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    if err := client.Ping(ctx); err != nil {
        log.Printf("Failed to ping Kafka brokers: %v", err)
        client.Close()
        return nil, fmt.Errorf("failed to ping Kafka brokers: %v", err)
    }
    log.Printf("Successfully pinged Kafka brokers")

    return &kafkaBroker{
        client: client,
        topic:  topic,
    }, nil
}

// Produce sends an event to the configured cloud provider
func (c *MultiCloudKafkaClient) Produce(ctx context.Context, event []byte) error {
    err := c.broker.Produce(ctx, event)
    if err != nil {
        log.Printf("Failed to produce event: %v", err)
        return err
    }
    log.Printf("Successfully produced event: %s", string(event))
    return nil
}

// Consume receives events from the configured cloud provider
func (c *MultiCloudKafkaClient) Consume(ctx context.Context, handler func([]byte) error) error {
    log.Printf("Starting consumer for topic: %s", c.config.KafkaTopic)
    return c.broker.Consume(ctx, handler)
}

// Close closes the connection to the event broker
func (c *MultiCloudKafkaClient) Close() error {
    return c.broker.Close()
}

// KafkaBroker implementations
func (b *kafkaBroker) Produce(ctx context.Context, event []byte) error {
    record := &kgo.Record{
        Topic: b.topic,
        Value: event,
    }
    err := b.client.ProduceSync(ctx, record).FirstErr()
    if err != nil {
        log.Printf("Produce error in broker: %v", err)
        return err
    }
    log.Printf("ProduceSync completed successfully")
    return nil
}

func (b *kafkaBroker) Consume(ctx context.Context, handler func([]byte) error) error {
    log.Printf("Starting to consume from topic: %s", b.topic)
    for {
        fetches := b.client.PollFetches(ctx)
        log.Printf("Polled fetches, num records: %d", len(fetches.Records()))
        if err := fetches.Err(); err != nil {
            if err == context.Canceled {
                log.Printf("Consumer context cancelled")
                return nil
            }
            log.Printf("Consume error: %v", err)
            return fmt.Errorf("consume error: %v", err)
        }

        fetches.EachRecord(func(r *kgo.Record) {
            log.Printf("Received message: %s (partition: %d, offset: %d)", string(r.Value), r.Partition, r.Offset)
            if err := handler(r.Value); err != nil {
                log.Printf("Handler error: %v", err)
            }
        })
        if len(fetches.Records()) == 0 {
            log.Printf("No records fetched, continuing to poll")
        }
    }
}

func (b *kafkaBroker) Close() error {
    b.client.Close()
    log.Printf("Kafka broker closed successfully")
    return nil
}