package processor

import (
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/publisher"
	"github.com/maksimru/event-scheduler/storage"
	log "github.com/sirupsen/logrus"
	"time"
)

type Processor struct {
	publisher   publisher.Publisher
	dataStorage *storage.PqStorage
}

func (p *Processor) Process() error {
	for {
		now := int(time.Now().Unix())
		if p.dataStorage.CheckScheduled(now) {
			msg := p.dataStorage.Dequeue()
			log.Trace("processor message is ready for delivery: scheduled for ", msg.GetPriority(), " at ", now)
			err := p.publisher.Push(message.NewMessage(msg.GetValue(), msg.GetPriority()))
			if err != nil {
				log.Error("processor message publish exception: scheduled for ", msg.GetPriority(), " at ", now, " ", err.Error())
				return err
			} else {
				log.Trace("processor message published: scheduled for ", msg.GetPriority(), " at ", now)
			}
		}
	}
}

func (p *Processor) Boot(publisher publisher.Publisher, dataStorage *storage.PqStorage) error {
	p.publisher = publisher
	p.dataStorage = dataStorage
	return nil
}
