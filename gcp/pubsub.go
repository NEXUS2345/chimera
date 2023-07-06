package gcp

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"google.golang.org/api/option"
)

// PubSubClient is a wrapper for pubsub.Client which provides a simple
// interface for publishing messages to a topic or subscribing to a topic.
type PubSubClient struct {
	// client is the pubsub.Client used to publish messages.
	client *pubsub.Client
	// projectID is the project ID of the project that the client is associated with.
	projectID string
	// The topic that the client will publish messages to.
	topic *pubsub.Topic
	// The subscription that the client will receive messages from.
	subscription *pubsub.Subscription
	// ctx is the context used to create the client.
	ctx context.Context
	// The options used to create the client.
	options option.ClientOption
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
	if options != nil {
		opts := *options
		client, err := pubsub.NewClient(ctx, projectID, opts)

		if err != nil {
			return nil, err
		}

		return &PubSubClient{
			client:    client,
			projectID: projectID,
			ctx:       ctx,
			options:   opts,
		}, nil
	} else {
		client, err := pubsub.NewClient(ctx, projectID)

		if err != nil {
			return nil, err
		}

		return &PubSubClient{
			client:    client,
			projectID: projectID,
			ctx:       ctx,
		}, nil
	}
}

// WriteMessage publishes a message to the topic that the client is associated with.
// It accepts the message to be published in string format, a pointer to a map of attributes, and a pointer to an ordering key.
// If the pointer to the map of attributes is nil, the message will be published without any attributes.
// If the pointer to the ordering key is nil, the message will be published without an ordering key.
// If an error occurs while publishing the message, it will be returned.
// Parameters:
// - message: The message to be published in string format.
// - attributes: A pointer to a map of attributes.
// - orderingKey: A pointer to an ordering key in string format.
// Returns:
// - A pointer to a PublishResult object.
// - An error.
func (p *PubSubClient) WriteMessage(message string, attributes *map[string]string, orderingKey *string) (*pubsub.PublishResult, error) {
	if p != nil {
		return nil, errors.New("PubSubClient is nil")
	}

	result := p.topic.Publish(p.ctx, &pubsub.Message{
		Data:        []byte(message),
		Attributes:  *attributes,
		OrderingKey: *orderingKey,
	})

	return result, nil
}
