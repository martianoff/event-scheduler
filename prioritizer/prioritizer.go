package prioritizer

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/storage"
	log "github.com/sirupsen/logrus"
	"time"
)

type Prioritizer struct {
	inboundPool *goconcurrentqueue.FIFO
	dataStorage *storage.PqStorage
	context     context.Context
}

func (p *Prioritizer) Process() error {
	for {
		select {
		case <-p.context.Done():
			log.Warn("prioritizer is stopped")
			return nil
		default:
		}
		if p.inboundPool.GetLen() == 0 {
			log.Trace("prioritizer queue is empty")
			time.Sleep(time.Second)
			continue
		}
		msg, _ := p.inboundPool.Dequeue()
		prioritizedMsg := msg.(message.Message)
		p.dataStorage.Enqueue(prioritizedMsg)
		log.Trace("prioritizer message pushed to the storage: ", prioritizedMsg.GetBody(), " scheduled at ", prioritizedMsg.GetAvailableAt())
	}
}

func (p *Prioritizer) Boot(ctx context.Context, inboundPool *goconcurrentqueue.FIFO, dataStorage *storage.PqStorage) error {
	p.context = ctx
	p.inboundPool = inboundPool
	p.dataStorage = dataStorage
	return nil
}
