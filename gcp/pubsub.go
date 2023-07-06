package gcp

import (
	"context"
	"google.golang.org/api/pubsub/v1"
)

type PubSubService struct {
	service      *pubsub.Service
	project      string
	topic        string
	subscription string
	ctx          context.Context
	err          error
}

func NewPubSubService(project, topic, subscription string, ctx context.Context) *PubSubService {
	service, err := pubsub.NewService(ctx)

	return &PubSubService{
		service:      service,
		project:      project,
		topic:        topic,
		subscription: subscription,
		ctx:          ctx,
		err:          err,
	}
}

func (p *PubSubService) WriteMessage(message string) (resp *pubsub.PublishResponse, err error) {
	if p.err != nil {
		return
	}

	do := &pubsub.PublishResponse{}

	// publish the message to the topic
	do, err = p.service.Projects.Topics.Publish(p.topic, &pubsub.PublishRequest{
		// create a pubsub message with the data
		Messages: []*pubsub.PubsubMessage{
			{
				Data: message,
			},
		},
	}).Do()
	if err != nil {
		return nil, err
	}

	return do, nil
}
