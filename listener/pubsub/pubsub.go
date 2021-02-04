package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/message"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"runtime"
	"strconv"
)

type Listener struct {
	config      config.Config
	inboundPool *goconcurrentqueue.FIFO
	client      *pubsub.Client
	context     context.Context
	cluster     *raft.Raft
}

func (l *Listener) Boot(ctx context.Context, config config.Config, inboundPool *goconcurrentqueue.FIFO, cluster *raft.Raft) error {
	l.config = config
	l.inboundPool = inboundPool
	client, err := makePubsubClient(ctx, config)
	l.client, l.context, l.cluster = client, ctx, cluster
	return err
}

func (l *Listener) SetPubsubClient(client *pubsub.Client) {
	l.client = client
}

func makePubsubClient(ctx context.Context, config config.Config) (*pubsub.Client, error) {
	client, err := pubsub.NewClient(ctx, config.PubsubListenerProjectID, option.WithCredentialsFile(config.PubsubListenerKeyFile))
	if err != nil {
		log.Error("listener client boot failure: ", err.Error())
		return nil, err
	}
	return client, err
}

func (l *Listener) Listen() error {
	defer func() {
		err := l.client.Close()
		if err != nil {
			log.Error("listener client termination failure: ", err.Error())
		}
	}()

	sub := l.client.Subscription(l.config.PubsubListenerSubscriptionID)
	sub.ReceiveSettings.Synchronous = false
	sub.ReceiveSettings.NumGoroutines = runtime.NumCPU()

	// Dedicated context
	pubsubContext, cancelListener := context.WithCancel(l.context)
	defer cancelListener()

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
				} else {
					err = l.inboundPool.Enqueue(message.NewMessage(string(msg.Data), priority))
					if err != nil {
						log.Error("listener inbound pool enqueue exception: ", err.Error())
					}
				}
			}
			msg.Ack()
		}
	}()

	err := sub.Receive(pubsubContext, func(ctx context.Context, msg *pubsub.Message) {
		cm <- msg
	})

	if err != nil {
		log.Error("listener message receive exception: ", err.Error())
		return err
	}

	return nil
}
