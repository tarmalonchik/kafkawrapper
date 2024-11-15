package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	segmentio "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/sirupsen/logrus"
)

type closures interface {
	Close() error
}

type Client struct {
	conf     Config
	closures []closures
}

func NewClient(conf Config) *Client {
	return &Client{
		conf: conf,
	}
}

func (c *Client) NewReader(ctx context.Context, topic, groupID string, partitions int) (*Reader, error) {
	if err := c.createTopic(topic, partitions); err != nil {
		return nil, errors.New("creating reader")
	}

	brokers := make([]string, c.conf.KafkaControllersCount)
	for i := range brokers {
		brokers[i] = fmt.Sprintf("%s:%s", fmt.Sprintf(c.conf.KafkaBrokerURLTemplate, i), c.conf.KafkaPort)
	}

	kafReader := segmentio.NewReader(
		segmentio.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
			Dialer: &segmentio.Dialer{
				Timeout:   timeout,
				DualStack: true,
				SASLMechanism: plain.Mechanism{
					Username: c.conf.KafkaUser,
					Password: c.conf.KafkaPassword,
				},
			},
		})
	c.closures = append(c.closures, kafReader)

	return newReader(ctx, kafReader), nil
}

// NewFetcher for manual commiting messages
func (c *Client) NewFetcher(ctx context.Context, topic, groupID string, partitions int) (*Fetcher, error) {
	if err := c.createTopic(topic, partitions); err != nil {
		return nil, errors.New("creating reader")
	}

	brokers := make([]string, c.conf.KafkaControllersCount)
	for i := range brokers {
		brokers[i] = fmt.Sprintf("%s:%s", fmt.Sprintf(c.conf.KafkaBrokerURLTemplate, i), c.conf.KafkaPort)
	}

	kafReader := segmentio.NewReader(
		segmentio.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
			Dialer: &segmentio.Dialer{
				Timeout:   timeout,
				DualStack: true,
				SASLMechanism: plain.Mechanism{
					Username: c.conf.KafkaUser,
					Password: c.conf.KafkaPassword,
				},
			},
		})
	c.closures = append(c.closures, kafReader)

	return newFetcher(ctx, kafReader), nil
}

func (c *Client) NewWriter(topic string, partitions int) (*Writer, error) {
	if err := c.createTopic(topic, partitions); err != nil {
		return nil, errors.New("creating reader")
	}

	kafWriter := &segmentio.Writer{
		Addr:                   segmentio.TCP(fmt.Sprintf("%s:%s", c.conf.KafkaHeadlessServiceURL, c.conf.KafkaPort)),
		Topic:                  topic,
		MaxAttempts:            10,
		WriteBackoffMin:        100 * time.Millisecond,
		WriteBackoffMax:        1 * time.Second,
		BatchTimeout:           100 * time.Millisecond,
		RequiredAcks:           segmentio.RequireOne,
		BatchBytes:             1000000,
		AllowAutoTopicCreation: false,
		Transport: &segmentio.Transport{
			SASL: plain.Mechanism{
				Username: c.conf.KafkaUser,
				Password: c.conf.KafkaPassword,
			},
		},
	}
	c.closures = append(c.closures, kafWriter)
	return newWriter(kafWriter), nil
}

func (c *Client) GracefulStop(ctx context.Context) error {
	<-ctx.Done()
	for i := range c.closures {
		if err := c.closures[i].Close(); err != nil {
			logrus.Errorf("error closing writer/reader: %v", err)
		}
	}
	return nil
}

func (c *Client) createTopic(topic string, partitionsCount int) error {
	dialer := &segmentio.Dialer{
		Timeout:       timeout,
		DualStack:     true,
		SASLMechanism: plain.Mechanism{Username: c.conf.KafkaUser, Password: c.conf.KafkaPassword},
	}

	conn, err := dialer.Dial(network, fmt.Sprintf("%s:%s", c.conf.KafkaHeadlessServiceURL, c.conf.KafkaPort))
	if err != nil {
		return errors.New("connecting to kafka during topic creation")
	}
	defer func() { _ = conn.Close() }()

	replicationFactor := c.conf.KafkaControllersCount - 1
	if replicationFactor == 0 {
		replicationFactor = 1
	}

	topicConf := segmentio.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitionsCount,
		ReplicationFactor: replicationFactor,
	}

	if err = conn.CreateTopics(topicConf); err != nil {
		return errors.New("creating topic")
	}
	return nil
}
