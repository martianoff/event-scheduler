package processor

import (
	"context"
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/dispatcher"
	"github.com/maksimru/event-scheduler/fsm"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/publisher"
	"github.com/maksimru/event-scheduler/storage"
	log "github.com/sirupsen/logrus"
	"time"
)

type Processor struct {
	publisher   publisher.Publisher
	dispatcher  dispatcher.Dispatcher
	dataStorage *storage.PqStorage
	context     context.Context
	time        CurrentTimeChecker
	cluster     *raft.Raft
	channel     channel.Channel
	stopFunc    context.CancelFunc
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

func (p *Processor) Stop() error {
	if p.stopFunc != nil {
		log.Info("processor stop called")
		p.stopFunc()
	}
	return nil
}

func (p *Processor) Process() error {
	ctx, cancelListener := context.WithCancel(p.context)
	defer cancelListener()
	p.stopFunc = cancelListener
	for {
		select {
		case <-ctx.Done():
			log.Warn("processor is stopped")
			return nil
		default:
		}
		now := int(p.time.Now().Unix())
		chStorage, chExists := p.dataStorage.GetChannelStorage(p.channel.ID)
		if !chExists {
			log.Warn("channel storage is not found (channel ", p.channel.ID, ") - processor is stopped")
			return nil
		}
		if p.cluster.State() == raft.Leader && chStorage.CheckScheduled(now) {
			// dequeue through FSM
			opPayload := fsm.CommandPayload{
				ChannelID: p.channel.ID,
				Operation: fsm.OperationMessagePop,
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

			err = p.dispatcher.Push(message.NewMessage(msg.GetBody(), msg.GetAvailableAt()), p.channel.ID)
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

func (p *Processor) Boot(ctx context.Context, dispatcher dispatcher.Dispatcher, dataStorage *storage.PqStorage, cluster *raft.Raft, channel channel.Channel) error {
	p.context = ctx
	p.dispatcher = dispatcher
	p.dataStorage = dataStorage
	p.time = RealTime{}
	p.cluster = cluster
	p.channel = channel
	return nil
}
