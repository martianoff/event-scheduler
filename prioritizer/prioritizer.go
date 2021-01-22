package prioritizer

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/maksimru/go-hpds/priorityqueue"
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
		message, _ := p.inboundPool.Dequeue()
		prioritizedMsg := message.(priorityqueue.StringPrioritizedValue)
		p.dataStorage.Enqueue(prioritizedMsg)
		log.Trace("prioritizer message pushed to the storage: ", prioritizedMsg.GetValue(), " scheduled at ", prioritizedMsg.GetPriority())
	}
}

func (p *Prioritizer) Boot(ctx context.Context, inboundPool *goconcurrentqueue.FIFO, dataStorage *storage.PqStorage) error {
	p.context = ctx
	p.inboundPool = inboundPool
	p.dataStorage = dataStorage
	return nil
}
