package dispatcher

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/publisher"
	publisherpubsub "github.com/maksimru/event-scheduler/publisher/pubsub"
	publishertest "github.com/maksimru/event-scheduler/publisher/test"
	"github.com/maksimru/event-scheduler/storage"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const DispatcherThreads = 5

type Dispatcher interface {
	Push(msg message.Message, channelID string) error
	Dispatch() error
}

type MessageDispatcher struct {
	outboundPool *goconcurrentqueue.FIFO
	context      context.Context
	publishers   map[string]publisher.Publisher
	dataStorage  *storage.PqStorage
}

type MessageForDelivery struct {
	msg       message.Message
	channelID string
}

func (m MessageForDelivery) GetMessage() message.Message {
	return m.msg
}

func NewDispatcher(ctx context.Context, outboundPool *goconcurrentqueue.FIFO, dataStorage *storage.PqStorage) *MessageDispatcher {
	return &MessageDispatcher{
		context:      ctx,
		outboundPool: outboundPool,
		publishers:   make(map[string]publisher.Publisher),
		dataStorage:  dataStorage,
	}
}

func (d *MessageDispatcher) SetPublisher(channelID string, p publisher.Publisher) {
	d.publishers[channelID] = p
}

func (d *MessageDispatcher) getChannelPublisher(channelID string) (publisher.Publisher, error) {
	p, has := d.publishers[channelID]
	if has {
		return p, nil
	}

	c, err := d.dataStorage.GetChannel(channelID)
	if err != nil {
		return nil, err
	}
	cfg := c.Destination

	switch cfg.Driver {
	case "pubsub":
		p = publisherpubsub.NewPubSubPublisher(d.context, cfg.Config)
	case "test":
		p = publishertest.NewTestPublisher()
	case "test_w":
		p = publishertest.NewTestPublisher().WrongConfig()
	default:
		log.Warn("selected publisher driver is not yet supported")
		panic("selected publisher driver is not yet supported")
	}
	d.publishers[channelID] = p
	return p, nil
}

func (d *MessageDispatcher) Push(msg message.Message, channelID string) error {
	err := d.outboundPool.Enqueue(MessageForDelivery{
		msg:       msg,
		channelID: channelID,
	})

	if err != nil {
		log.Error("dispatcher outbound pool push exception: ", err.Error())
		return err
	}

	return nil
}

func (d *MessageDispatcher) Dispatch() error {
	defer func() {
		// close opened publisher connections
		for _, p := range d.publishers {
			_ = p.Close()
		}
	}()
	var dispatcherWg sync.WaitGroup
	for threadID := 0; threadID < DispatcherThreads; threadID++ {
		dispatcherWg.Add(1)

		go func(threadId int) {
			defer dispatcherWg.Done()

			for {
				select {
				case <-d.context.Done():
					log.Warn("dispatcher is stopped")
					return
				default:
				}

				if d.outboundPool.GetLen() == 0 {
					log.Tracef("dispatcher queue is empty, thread (%v)", threadId)
					time.Sleep(time.Second)
					continue
				}

				v, _ := d.outboundPool.DequeueOrWaitForNextElement()
				m := v.(MessageForDelivery)
				log.Tracef("dispatcher dequeued element, thread (%v): %v", threadId, m.msg.GetBody())

				p, err := d.getChannelPublisher(m.channelID)
				if err != nil {
					log.Warn("dispatcher can't init publisher for channel: ", m.channelID)
					continue
				}

				result := p.Dispatch(m.GetMessage())

				// delivery failed, redeliver
				if result != nil {
					// Attempt to redeliver the message
					err := d.outboundPool.Enqueue(m)
					if err != nil {
						log.Errorf("dispatcher message redeliver exception, thread (%v): %v", threadId, err.Error())
					} else {
						log.Tracef("dispatcher message redelivered, thread (%v)", threadId)
					}
				}

			}
		}(threadID)
	}
	dispatcherWg.Wait()
	return nil
}
