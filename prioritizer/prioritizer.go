package prioritizer

import (
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/maksimru/go-hpds/priorityqueue"
	log "github.com/sirupsen/logrus"
	"time"
)

type Prioritizer struct {
	inboundPool *goconcurrentqueue.FIFO
	dataStorage *storage.PqStorage
}

func (p *Prioritizer) Process() error {
	for {
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

func (p *Prioritizer) Boot(inboundPool *goconcurrentqueue.FIFO, dataStorage *storage.PqStorage) error {
	p.inboundPool = inboundPool
	p.dataStorage = dataStorage
	return nil
}
