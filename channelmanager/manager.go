package channelmanager

import (
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/errormessages"
	"github.com/maksimru/event-scheduler/fsm"
	"github.com/maksimru/event-scheduler/storage"
	log "github.com/sirupsen/logrus"
	"time"
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
	handler *channel.EventHandler
}

func NewSchedulerChannelManager(cluster *raft.Raft, storage *storage.PqStorage, handler *channel.EventHandler) *SchedulerChannelManager {
	if handler == nil {
		handler = channel.NewEventHandler(func(c channel.Channel) {}, func(c channel.Channel) {}, func(c channel.Channel) {})
	}
	return &SchedulerChannelManager{
		cluster: cluster,
		storage: storage,
		handler: handler,
	}
}

func (m *SchedulerChannelManager) BootChannelManager(cluster *raft.Raft, storage *storage.PqStorage, handler *channel.EventHandler) error {
	m.cluster = cluster
	m.storage = storage
	m.handler = handler
	return nil
}

func (m *SchedulerChannelManager) GetChannels() ([]channel.Channel, error) {
	if m.cluster.State() != raft.Leader {
		return nil, errormessages.ErrOperationIsRestrictedOnNonLeader
	}
	return m.storage.GetChannels(), nil
}

func (m *SchedulerChannelManager) AddChannel(channelInput channel.Channel) (*channel.Channel, error) {
	if m.cluster.State() != raft.Leader {
		return nil, errormessages.ErrOperationIsRestrictedOnNonLeader
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
	r, _ := applyFuture.Response().(*fsm.ApplyResponse)
	if r.Err != nil {
		return nil, r.Err
	}
	c := r.Data.(channel.Channel)
	m.handler.OnAdded(c)
	return &c, nil
}

func (m *SchedulerChannelManager) DeleteChannel(ID string) error {
	if m.cluster.State() != raft.Leader {
		return errormessages.ErrOperationIsRestrictedOnNonLeader
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
	r, _ := applyFuture.Response().(*fsm.ApplyResponse)
	if r.Err != nil {
		return r.Err
	}
	c := r.Data.(channel.Channel)
	m.handler.OnDeleted(c)
	return nil
}

func (m *SchedulerChannelManager) UpdateChannel(ID string, channelInput channel.Channel) (*channel.Channel, error) {
	if m.cluster.State() != raft.Leader {
		return nil, errormessages.ErrOperationIsRestrictedOnNonLeader
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
	r, _ := applyFuture.Response().(*fsm.ApplyResponse)
	if r.Err != nil {
		return nil, r.Err
	}
	c := r.Data.(channel.Channel)
	m.handler.OnUpdated(c)
	return &c, nil
}
