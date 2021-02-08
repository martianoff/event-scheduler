package processor

import (
	"context"
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/fsm"
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
	cluster     *raft.Raft
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
		if p.cluster.State() == raft.Leader && p.dataStorage.CheckScheduled(now) {
			// dequeue through FSM
			opPayload := fsm.CommandPayload{
				Operation: fsm.OperationPop,
				Value:     nil,
			}
			opPayloadData, err := json.Marshal(opPayload)
			if err != nil {
				log.Error("processor error preparing saving data payload: ", err.Error())
				continue
			}
			applyFuture := p.cluster.Apply(opPayloadData, 500*time.Millisecond)
			if err := applyFuture.Error(); err != nil {
				log.Error("processor error persisting data in raft cluster: ", err.Error())
				continue
			}
			clusterResponse, ok := applyFuture.Response().(*fsm.ApplyResponse)
			if !ok {
				log.Error("processor error parsing apply response")
				continue
			}

			msg := clusterResponse.Data.(message.Message)

			log.Trace("processor message is ready for delivery: scheduled for ", msg.GetAvailableAt(), " at ", now)

			err = p.publisher.Push(message.NewMessage(msg.GetBody(), msg.GetAvailableAt()))
			if err != nil {
				log.Error("processor message publish exception: scheduled for ", msg.GetAvailableAt(), " at ", now, " ", err.Error())
				return err
			}
			log.Trace("processor message published: scheduled for ", msg.GetAvailableAt(), " at ", now)
		} else {
			time.Sleep(time.Second)
		}
	}
}

func (p *Processor) Boot(ctx context.Context, publisher publisher.Publisher, dataStorage *storage.PqStorage, cluster *raft.Raft) error {
	p.context = ctx
	p.publisher = publisher
	p.dataStorage = dataStorage
	p.time = RealTime{}
	p.cluster = cluster
	return nil
}
