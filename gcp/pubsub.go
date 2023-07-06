package gcp

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"google.golang.org/api/option"
)

type PubSubClient struct {
	client       *pubsub.Client
	projectID    string
	topic        *pubsub.Topic
	subscription *pubsub.Subscription
	ctx          context.Context
	options      option.ClientOption
}

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

func (p *PubSubClient) WriteMessage(message string) (*pubsub.PublishResult, error) {
	if p != nil {
		return nil, errors.New("PubSubClient is nil")
	}

	result := p.topic.Publish(p.ctx, &pubsub.Message{
		Data: []byte(message),
	})

	return result, nil
}
