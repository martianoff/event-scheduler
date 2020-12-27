package listener_pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/go-hpds/priorityqueue"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"strconv"
)

type ListenerPubsub struct {
	config       config.Config
	inboundPool  *goconcurrentqueue.FIFO
	pubsubClient *pubsub.Client
}

func (l *ListenerPubsub) Boot(config config.Config, inboundPool *goconcurrentqueue.FIFO) error {
	l.config = config
	l.inboundPool = inboundPool
	return nil
}

func (l *ListenerPubsub) Listen() error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, l.config.PubsubListenerProjectID, option.WithCredentialsFile(l.config.PubsubListenerKeyFile))
	if err != nil {
		log.Error("listener client creation failure: ", err.Error())
		return err
	}
	defer func() {
		err := client.Close()
		if err != nil {
			log.Error("listener client termination failure: ", err.Error())
		}
	}()

	sub := client.Subscription(l.config.PubsubListenerSubscriptionID)

	// Create a channel to handle messages to as they come in.
	cm := make(chan *pubsub.Message)
	defer close(cm)

	// Handle individual messages in a goroutine.
	go func() {
		for msg := range cm {
			log.Trace("listener message received: ", string(msg.Data))
			if availableAt, has := msg.Attributes["available_at"]; has {
				priority, err := strconv.Atoi(availableAt)
				if err != nil {
					log.Error("listener unable to read available_at attribute: ", err.Error())
				}
				err = l.inboundPool.Enqueue(priorityqueue.NewStringPrioritizedValue(string(msg.Data), priority))
				if err != nil {
					log.Error("listener inbound pool enqueue exception: ", err.Error())
				}
			}
			msg.Ack()
		}
	}()

	// Receive blocks until the context is cancelled or an error occurs.
	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		cm <- msg
	})

	if err != nil {
		log.Error("listener message receive exception: ", err.Error())
		return err
	}

	return nil
}
