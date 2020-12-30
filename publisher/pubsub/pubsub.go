package pubsubpublisher

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/message"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"sync"
	"sync/atomic"
	"time"
)

const DispatcherThreads = 5

type PubsubPublisher struct {
	config       config.Config
	outboundPool *goconcurrentqueue.FIFO
	client       *pubsub.Client
	context      context.Context
}

func (p *PubsubPublisher) Boot(config config.Config, outboundPool *goconcurrentqueue.FIFO) error {
	p.config = config
	p.outboundPool = outboundPool
	ctx := context.Background()
	client, err := makePubsubClient(ctx, config)
	p.client, p.context = client, ctx
	return err
}

func makePubsubClient(ctx context.Context, config config.Config) (*pubsub.Client, error) {
	client, err := pubsub.NewClient(ctx, config.PubsubPublisherProjectID, option.WithCredentialsFile(config.PubsubPublisherKeyFile))
	if err != nil {
		log.Error("publisher client creation failure: ", err.Error())
		return nil, err
	}
	return client, err
}

func (p *PubsubPublisher) Push(msg message.Message) error {
	err := p.outboundPool.Enqueue(msg)

	if err != nil {
		log.Error("publisher outbound pool push exception: ", err.Error())
		return err
	}

	return nil
}

func (p PubsubPublisher) Dispatch() error {
	defer func() {
		err := p.client.Close()
		if err != nil {
			log.Error("publisher client termination failure: ", err.Error())
		}
	}()

	var totalErrors uint64

	t := p.client.Topic(p.config.PubsubPublisherTopicID)

	var dispatcherWg sync.WaitGroup
	for threadID := 0; threadID < DispatcherThreads; threadID++ {
		dispatcherWg.Add(1)

		go func(threadId int) {
			defer dispatcherWg.Done()

			for {
				select {
				case <-p.context.Done():
					return
				default:
				}

				if p.outboundPool.GetLen() == 0 {
					log.Tracef("publisher queue is empty, thread (%v)", threadId)
					time.Sleep(time.Second)
					continue
				}

				v, _ := p.outboundPool.DequeueOrWaitForNextElement()
				msg := v.(message.Message)
				log.Tracef("publisher dequeued element, thread (%v): %v", threadId, msg.GetBody())

				result := t.Publish(p.context, &pubsub.Message{
					Data: []byte(msg.GetBody().(string)),
				})

				var wg sync.WaitGroup
				wg.Add(1)

				go func(threadId int, res *pubsub.PublishResult, attemptedMessage message.Message) {
					defer wg.Done()
					id, err := res.Get(p.context)
					if err != nil {
						// Error handling code can be added here.
						log.Warnf("publisher message delivery exception, thread (%v): %v", threadId, err.Error())
						atomic.AddUint64(&totalErrors, 1)
						// Attempt to redeliver the message
						err = p.outboundPool.Enqueue(attemptedMessage)
						if err != nil {
							log.Errorf("publisher message redeliver exception, thread (%v): %v", threadId, err.Error())
						} else {
							log.Tracef("publisher message redelivered, thread (%v)", threadId)
						}
						return
					}
					log.Tracef("publisher message published, thread (%v): %v", threadId, id)
				}(threadId, result, msg)
				wg.Wait()
			}
		}(threadID)
	}
	dispatcherWg.Wait()
	return nil
}
