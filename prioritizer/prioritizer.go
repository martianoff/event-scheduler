package prioritizer

import (
	"encoding/json"
	"errors"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/fsm"
	"github.com/maksimru/event-scheduler/message"
	log "github.com/sirupsen/logrus"
	"time"
)

type Prioritizer struct {
	cluster *raft.Raft
}

func (p *Prioritizer) Persist(persistedMsg message.Message, channel channel.Channel) error {
	// push through FSM
	opPayload := fsm.CommandPayload{
		Operation: fsm.OperationMessagePush,
		Message:   persistedMsg,
		ChannelID: channel.ID,
	}
	opPayloadData, err := json.Marshal(opPayload)
	if err != nil {
		log.Error("prioritizer error preparing saving data payload: ", err.Error())
		return err
	}
	applyFuture := p.cluster.Apply(opPayloadData, 500*time.Millisecond)
	if err := applyFuture.Error(); err != nil {
		log.Error("prioritizer error persisting data in raft cluster: ", err.Error())
		return err
	}
	_, ok := applyFuture.Response().(*fsm.ApplyResponse)
	if !ok {
		log.Error("prioritizer error parsing apply response")
		return errors.New("fsm response failed")
	}
	return nil
}

func (p *Prioritizer) Boot(cluster *raft.Raft) error {
	p.cluster = cluster
	return nil
}
