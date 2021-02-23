package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/maksimru/event-scheduler/channel"
	pubsubconfig "github.com/maksimru/event-scheduler/listener/config"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/prioritizer"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"runtime"
	"strconv"
)

type Listener struct {
	config      pubsubconfig.SourceConfig
	client      *pubsub.Client
	context     context.Context
	stopFunc    context.CancelFunc
	prioritizer *prioritizer.Prioritizer
	channel     channel.Channel
}

func (l *Listener) Boot(ctx context.Context, channel channel.Channel, prioritizer *prioritizer.Prioritizer) error {
	l.context, l.config = ctx, channel.Source.Config.(pubsubconfig.SourceConfig)
	client, err := makePubsubClient(l.context, l.config)
	l.client, l.prioritizer, l.channel = client, prioritizer, channel
	return err
}

func (l *Listener) SetPubsubClient(client *pubsub.Client) {
	l.client = client
}

func makePubsubClient(ctx context.Context, config pubsubconfig.SourceConfig) (*pubsub.Client, error) {
	client, err := pubsub.NewClient(ctx, config.ProjectID, option.WithCredentialsFile(config.KeyFile))
	if err != nil {
		log.Error("listener client boot failure: ", err.Error())
		return nil, err
	}
	return client, err
}

func (l *Listener) Stop() error {
	if l.stopFunc != nil {
		log.Info("listener stop called")
		l.stopFunc()
	}
	return nil
}

func (l *Listener) Listen() error {
	defer func() {
		err := l.client.Close()
		if err != nil {
			log.Error("listener client termination failure: ", err.Error())
		}
	}()

	sub := l.client.Subscription(l.config.SubscriptionID)
	sub.ReceiveSettings.Synchronous = false
	sub.ReceiveSettings.NumGoroutines = runtime.NumCPU()

	// Dedicated context
	pubsubContext, cancelListener := context.WithCancel(l.context)
	defer cancelListener()
	l.stopFunc = cancelListener

	// Create a channel to handle messages to as they come in.
	cm := make(chan *pubsub.Message)
	defer close(cm)

	// Handle individual messages in a goroutine.
	go func() {
		for {
			select {
			case <-pubsubContext.Done():
				return
			case msg := <-cm:
				log.Trace("listener message received: ", string(msg.Data))
				if availableAt, has := msg.Attributes["available_at"]; has {
					priority, err := strconv.Atoi(availableAt)
					if err != nil {
						log.Error("listener unable to read available_at attribute: ", err.Error())
					} else {
						err := l.prioritizer.Persist(message.NewMessage(string(msg.Data), priority), l.channel)
						if err != nil {
							log.Warn("listener is unable to persist received message")
							// do not ack the message for strong consistency
							msg.Nack()
							break
						}
					}
				}
				msg.Ack()
			}
		}
	}()

	err := sub.Receive(pubsubContext, func(ctx context.Context, msg *pubsub.Message) {
		cm <- msg
	})

	if err != nil {
		log.Error("listener message receive exception: ", err.Error())
		return err
	}

	log.Info("listener stopped")

	return nil
}
