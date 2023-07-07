package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

// PubSubClient is a wrapper for pubsub.Client which provides a simple
// interface for publishing messages to a topic or subscribing to a topic.
type PubSubClient struct {
	// client is the pubsub.Client used to publish messages.
	client *pubsub.Client
	// projectID is the project ID of the project that the client is associated with.
	projectID string
	// The topics that the client will publish messages to or receive messages from. The key is the topic ID and the value is the topic object.
	topics map[string]*pubsub.Topic
	// The subscriptions that the client will receive messages from. The key is the subscription ID and the value is the subscription object.
	subscriptions map[string]*pubsub.Subscription
	// ctx is the context used to create the client.
	ctx context.Context
	// The options used to create the client.
	options option.ClientOption
	// The status of the client. If the client is closed, the status will be true.
	closed bool
}

// NewPubSubClient creates a new PubSubClient and returns a pointer to it.
// It accepts the project ID of the project that the client will be associated with, a context, and an optional ClientOption object reference.
// If the ClientOption object reference is nil, the client will be created with the default options.
// If the ClientOption object reference is not nil, the client will be created with the options specified in the ClientOption object.
// If an error occurs while creating the client, it will be returned.
// Parameters:
// - projectID: The project ID of the project that the client will be associated with in string format.
// - ctx: The context used to create the client.
// - options: A pointer to a ClientOption object that specifies the options used to create the client.
// Returns:
// - A pointer to a PubSubClient object.
// - An error.
func NewPubSubClient(projectID string, ctx context.Context, options *option.ClientOption) (*PubSubClient, error) {
	if len(projectID) == 0 {
		return nil, errors.New("projectID cannot be empty")
	}
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	var opts option.ClientOption

	if options != nil {
		opts = *options
	}

	client, err := pubsub.NewClient(ctx, projectID, opts)

	if err != nil {
		return nil, err
	}

	// TODO: Validate that pubsub.NewClient will accept a nil opts parameter.

	return &PubSubClient{
		client:    client,
		projectID: projectID,
		ctx:       ctx,
		options:   opts,
	}, nil
}

// GracefulClose closes the client and stops publish operations in all topics associated with the client.
// All outstanding publish operations will be completed before the client is closed.
// If the PubSubClient object is nil, an error will be returned.
// If an error occurs while closing the client, it will be returned.
// Parameters:
// - None.
// Returns:
// - An error.
func (p *PubSubClient) GracefulClose() error {
	if p == nil {
		return errors.New("PubSubClient cannot be nil")
	}

	if len(p.topics) > 0 {
		for _, topic := range p.topics {
			topic.Stop()
		}
	}

	if err := p.client.Close(); err != nil {
		return err
	}

	p.closed = true
	return nil
}

// GetTopicsFromClient gets all topics available from the client and adds them to the topics map in the PubSubClient object.
// The topics will be retrieved from the project that the client is associated with.
// If the PubSubClient object is nil, an error will be returned.
// If an error occurs while getting the topics, it will be returned.
// Parameters:
// - ctx: The context used to get the topics.
// Returns:
// - An error.
func (p *PubSubClient) GetTopicsFromClient(ctx context.Context) error {
	if p == nil {
		return errors.New("PubSubClient cannot be nil")
	}
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}

	topics := make([]*pubsub.Topic, 0)
	it := p.client.Topics(ctx)
	for {
		topic, err := it.Next()
		if err == nil {
			topics = append(topics, topic)
		} else if err != nil {
			return err
		} else {
			break
		}
	}

	if len(topics) == 0 {
		return errors.New("no topics found")
	}

	p.topics = make(map[string]*pubsub.Topic)
	for _, topic := range topics {
		p.topics[topic.ID()] = topic
	}

	return nil
}

// OpenTopic opens a topic that the client will publish messages to or receive messages from.
// It accepts the topic ID in string format.
// It adds the topic to the topics map in the PubSubClient object.
// If the PubSubClient object is nil or the topic name is empty, an error will be returned.
// If the topic does not exist, an error will be returned.
// Parameters:
// - ctx: The context used to open the topic.
// - topicID: The ID of the topic in string format.
// Returns:
// - An error.
func (p *PubSubClient) OpenTopic(ctx context.Context, topicID string) error {
	if len(topicID) == 0 {
		return errors.New("topicID cannot be empty")
	}
	if p == nil {
		return errors.New("PubSubClient cannot be nil")
	}
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}

	p.topics[topicID] = p.client.Topic(topicID)
	t := p.topics[topicID]
	if _, err := t.Exists(ctx); err != nil {
		return err
	} else if exists, _ := t.Exists(ctx); !exists {
		return errors.New("topic does not exist")
	}

	return nil
}

// OpenManyTopics opens multiple topics that the client will publish messages to or receive messages from.
// It accepts a slice of topic IDs in string format.
// It adds the topics to the topics map in the PubSubClient object.
// If the PubSubClient object is nil or the slice of topic IDs is empty, an error will be returned.
// If any of the topics do not exist, an error will be returned.
// Parameters:
// - ctx: The context used to open the topics.
// - topicIDs: A slice of topic IDs in string format.
// Returns:
// - An error.
func (p *PubSubClient) OpenManyTopics(ctx context.Context, topicIDs []string) error {
	if len(topicIDs) == 0 {
		return errors.New("topicIDs cannot be empty")
	}
	if p == nil {
		return errors.New("PubSubClient cannot be nil")
	}
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}

	for _, topicID := range topicIDs {
		if err := p.OpenTopic(ctx, topicID); err != nil {
			return fmt.Errorf("topicID %s returned error: %s", topicID, err.Error())
		}
	}

	return nil
}

// OpenSubscription opens a subscription that the client will receive messages from.
// It accepts the subscription ID in string format.
// It adds the subscription to the subscriptions map in the PubSubClient object.
// If the PubSubClient object is nil or the subscription name is empty, an error will be returned.
// If the subscription does not exist, an error will be returned.
// Parameters:
// - ctx: The context used to open the subscription.
// - subscriptionID: The name of the subscription in string format.
// Returns:
// - An error.
func (p *PubSubClient) OpenSubscription(ctx context.Context, subscriptionID string) error {
	if len(subscriptionID) == 0 {
		return errors.New("subscriptionID cannot be empty")
	}
	if p == nil {
		return errors.New("PubSubClient cannot be nil")
	}
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}

	p.subscriptions[subscriptionID] = p.client.Subscription(subscriptionID)
	s := p.subscriptions[subscriptionID]
	if _, err := s.Exists(ctx); err != nil {
		return err
	} else if exists, _ := s.Exists(ctx); !exists {
		return errors.New("subscription does not exist")
	}

	return nil
}

// OpenManySubscriptions opens multiple subscriptions that the client will receive messages from.
// It accepts a slice of subscription IDs in string format.
// It adds the subscriptions to the subscriptions map in the PubSubClient object.
// If the PubSubClient object is nil or the slice of subscription IDs is empty, an error will be returned.
// If any of the subscriptions do not exist, an error will be returned.
// Parameters:
// - ctx: The context used to open the subscriptions.
// - subscriptionIDs: A slice of subscription IDs in string format.
// Returns:
// - An error.
func (p *PubSubClient) OpenManySubscriptions(ctx context.Context, subscriptionIDs []string) error {
	if len(subscriptionIDs) == 0 {
		return errors.New("subscriptionIDs cannot be empty")
	}
	if p == nil {
		return errors.New("PubSubClient cannot be nil")
	}
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}

	for _, subscriptionID := range subscriptionIDs {
		if err := p.OpenSubscription(ctx, subscriptionID); err != nil {
			return fmt.Errorf("subscriptionID %s returned error: %s", subscriptionID, err.Error())
		}
	}

	return nil
}

// WriteMessage publishes a message to the topic that the client is associated with.
// It accepts the message to be published in string format, a pointer to a map of attributes, and a pointer to an ordering key.
// If the PubSubClient object is nil or the message is empty, an error will be returned.
// If the pointer to the map of attributes is nil, the message will be published without any attributes.
// If the pointer to the ordering key is nil, the message will be published without an ordering key.
// If an error occurs while publishing the message, it will be returned.
// Parameters:
// - ctx: The context used to publish the message.
// - message: The message to be published in string format.
// - attributes: A pointer to a map of attributes.
// - orderingKey: A pointer to an ordering key in string format.
// Returns:
// - A pointer to a PublishResult object.
// - An error.
func (p *PubSubClient) WriteMessage(ctx context.Context, topic string, message string, attributes *map[string]string, orderingKey *string) (*pubsub.PublishResult, error) {
	if p != nil {
		return nil, errors.New("PubSubClient cannot be nil")
	}
	if p.topics[topic] == nil {
		return nil, errors.New("topic does not exist in PubSubClient")
	}
	if len(message) == 0 {
		return nil, errors.New("message cannot be empty")
	}
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	t := p.topics[topic]

	result := t.Publish(ctx, &pubsub.Message{
		Data:        []byte(message),
		Attributes:  *attributes,
		OrderingKey: *orderingKey,
	})
	return result, nil
}

// WriteJsonMessage publishes a message in encoded JSON format to the topic that the client is associated with.
// It accepts the message to be published in encoded JSON ([]byte) format, a pointer to a map of attributes, and a pointer to an ordering key.
// If the PubSubClient object is nil or the JSON message is invalid, an error will be returned.
// If the pointer to the map of attributes is nil, the message will be published without any attributes.
// If the pointer to the ordering key is nil, the message will be published without an ordering key.
// If an error occurs while publishing the message, it will be returned.
// Parameters:
// - ctx: The context used to publish the message.
// - message: The message to be published in encoded JSON []byte format.
// - attributes: A pointer to a map of attributes.
// - orderingKey: A pointer to an ordering key in string format.
// Returns:
// - A pointer to a PublishResult object.
// - An error.
func (p *PubSubClient) WriteJsonMessage(ctx context.Context, topic string, message []byte, attributes *map[string]string, orderingKey *string) (*pubsub.PublishResult, error) {
	if p != nil {
		return nil, errors.New("PubSubClient cannot be nil")
	}
	if p.topics[topic] == nil {
		return nil, errors.New("topic does not exist in PubSubClient")
	}
	if message == nil {
		return nil, errors.New("message cannot be nil")
	}
	if len(message) == 0 {
		return nil, errors.New("message cannot be empty")
	}
	if !json.Valid(message) {
		return nil, errors.New("invalid json")
	}
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	t := p.topics[topic]

	result := t.Publish(ctx, &pubsub.Message{
		Data:        message,
		Attributes:  *attributes,
		OrderingKey: *orderingKey,
	})

	return result, nil
}
