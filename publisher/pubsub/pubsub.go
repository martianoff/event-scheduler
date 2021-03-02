package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/message"
	pubsubconfig "github.com/maksimru/event-scheduler/publisher/pubsub/config"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

type Publisher struct {
	config  pubsubconfig.DestinationConfig
	client  *pubsub.Client
	context context.Context
}

func NewPubSubPublisher(ctx context.Context, config channel.DestinationConfig) *Publisher {
	cfg := config.(pubsubconfig.DestinationConfig)
	psclient, err := makePubsubClient(ctx, cfg)
	if err != nil {
		panic("exception during publisher boot: " + err.Error())
	}
	return &Publisher{
		config:  cfg,
		client:  psclient,
		context: ctx,
	}
}

func (p *Publisher) SetPubsubClient(client *pubsub.Client) {
	p.client = client
}

func makePubsubClient(ctx context.Context, config pubsubconfig.DestinationConfig) (*pubsub.Client, error) {
	client, err := pubsub.NewClient(ctx, config.ProjectID, option.WithCredentialsFile(config.KeyFile))
	if err != nil {
		log.Error("publisher client boot failure: ", err.Error())
		return nil, err
	}
	return client, err
}

func (p *Publisher) Close() error {
	err := p.client.Close()
	if err != nil {
		log.Error("publisher client termination failure: ", err.Error())
		return err
	}
	return nil
}

func (p *Publisher) Dispatch(msg message.Message) error {
	t := p.client.Topic(p.config.TopicID)
	result := t.Publish(p.context, &pubsub.Message{
		Data: []byte(msg.GetBody().(string)),
	})

	id, err := result.Get(p.context)
	if err != nil {
		log.Warn("publisher message delivery exception:", err.Error())
		return err
	}
	log.Trace("publisher message published: ", id)

	return nil
}
