package processor

import (
	"context"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/publisher"
	"github.com/maksimru/event-scheduler/storage"
	log "github.com/sirupsen/logrus"
	"time"
)

type Processor struct {
	publisher   publisher.Publisher
	dataStorage *storage.PqStorage
	context     context.Context
	time        CurrentTimeChecker
}

func (p *Processor) SetTime(time CurrentTimeChecker) {
	p.time = time
}

type CurrentTimeChecker interface {
	Now() time.Time
}

type RealTime struct {
}

func (RealTime) Now() time.Time {
	return time.Now()
}

type MockTime struct {
	time time.Time
}

func NewMockTime(time time.Time) MockTime {
	return MockTime{time: time}
}

func (m MockTime) Now() time.Time {
	return m.time
}

func (p *Processor) Process() error {
	for {
		select {
		case <-p.context.Done():
			log.Warn("processor is stopped")
			return nil
		default:
		}
		now := int(p.time.Now().Unix())
		if p.dataStorage.CheckScheduled(now) {
			msg := p.dataStorage.Dequeue()
			log.Trace("processor message is ready for delivery: scheduled for ", msg.GetPriority(), " at ", now)
			err := p.publisher.Push(message.NewMessage(msg.GetValue(), msg.GetPriority()))
			if err != nil {
				log.Error("processor message publish exception: scheduled for ", msg.GetPriority(), " at ", now, " ", err.Error())
				return err
			}
			log.Trace("processor message published: scheduled for ", msg.GetPriority(), " at ", now)
		} else {
			time.Sleep(time.Second)
		}
	}
}

func (p *Processor) Boot(ctx context.Context, publisher publisher.Publisher, dataStorage *storage.PqStorage) error {
	p.context = ctx
	p.publisher = publisher
	p.dataStorage = dataStorage
	p.time = RealTime{}
	return nil
}
