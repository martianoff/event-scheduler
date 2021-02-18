package channelmanager

import (
	"encoding/json"
	"errors"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/fsm"
	"github.com/maksimru/event-scheduler/storage"
	log "github.com/sirupsen/logrus"
	"time"
)

var (
	ErrOperationIsRestrictedOnNonLeader = errors.New("channel operation is denied on non-leader node")
)

type ChannelManager interface {
	GetChannels() ([]channel.Channel, error)
	AddChannel(channel channel.Channel) (*channel.Channel, error)
	DeleteChannel(id string) error
	UpdateChannel(id string, channel channel.Channel) (*channel.Channel, error)
}

type SchedulerChannelManager struct {
	cluster *raft.Raft
	storage *storage.PqStorage
}

func (m *SchedulerChannelManager) BootChannelManager(cluster *raft.Raft, storage *storage.PqStorage) error {
	m.cluster = cluster
	m.storage = storage
	return nil
}

func (m *SchedulerChannelManager) GetChannels() ([]channel.Channel, error) {
	if m.cluster.State() != raft.Leader {
		return nil, ErrOperationIsRestrictedOnNonLeader
	}
	return m.storage.GetChannels(), nil
}

func (m *SchedulerChannelManager) AddChannel(channelInput channel.Channel) (*channel.Channel, error) {
	if m.cluster.State() != raft.Leader {
		return nil, ErrOperationIsRestrictedOnNonLeader
	}
	opPayload := fsm.CommandPayload{
		Operation: fsm.OperationChannelCreate,
		Channel:   channelInput,
	}
	opPayloadData, err := json.Marshal(opPayload)
	if err != nil {
		log.Error("channelmanager error preparing create data payload: ", err.Error())
		return nil, err
	}
	applyFuture := m.cluster.Apply(opPayloadData, 500*time.Millisecond)
	if err := applyFuture.Error(); err != nil {
		log.Error("channelmanager error persisting data in raft cluster: ", err.Error())
		return nil, err
	}
	r, ok := applyFuture.Response().(*fsm.ApplyResponse)
	if !ok {
		log.Error("channelmanager error parsing apply response")
		return nil, errors.New("fsm response failed")
	}
	c := r.Data.(channel.Channel)
	return &c, nil
}

func (m *SchedulerChannelManager) DeleteChannel(ID string) error {
	if m.cluster.State() != raft.Leader {
		return ErrOperationIsRestrictedOnNonLeader
	}
	opPayload := fsm.CommandPayload{
		Operation: fsm.OperationChannelDelete,
		ChannelID: ID,
	}
	opPayloadData, err := json.Marshal(opPayload)
	if err != nil {
		log.Error("channelmanager error preparing delete data payload: ", err.Error())
		return err
	}
	applyFuture := m.cluster.Apply(opPayloadData, 500*time.Millisecond)
	if err := applyFuture.Error(); err != nil {
		log.Error("channelmanager error persisting data in raft cluster: ", err.Error())
		return err
	}
	_, ok := applyFuture.Response().(*fsm.ApplyResponse)
	if !ok {
		log.Error("channelmanager error parsing apply response")
		return errors.New("fsm response failed")
	}
	return nil
}

func (m *SchedulerChannelManager) UpdateChannel(ID string, channelInput channel.Channel) (*channel.Channel, error) {
	if m.cluster.State() != raft.Leader {
		return nil, ErrOperationIsRestrictedOnNonLeader
	}
	opPayload := fsm.CommandPayload{
		Operation: fsm.OperationChannelUpdate,
		ChannelID: ID,
		Channel:   channelInput,
	}
	opPayloadData, err := json.Marshal(opPayload)
	if err != nil {
		log.Error("channelmanager error preparing update data payload: ", err.Error())
		return nil, err
	}
	applyFuture := m.cluster.Apply(opPayloadData, 500*time.Millisecond)
	if err := applyFuture.Error(); err != nil {
		log.Error("channelmanager error persisting data in raft cluster: ", err.Error())
		return nil, err
	}
	r, ok := applyFuture.Response().(*fsm.ApplyResponse)
	if !ok {
		log.Error("channelmanager error parsing apply response")
		return nil, errors.New("fsm response failed")
	}
	c := r.Data.(channel.Channel)
	return &c, nil
}
