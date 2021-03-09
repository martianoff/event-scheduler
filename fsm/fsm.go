package fsm

import (
	"bufio"
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/channel"
	pubsublistenerconfig "github.com/maksimru/event-scheduler/listener/pubsub/config"
	"github.com/maksimru/event-scheduler/message"
	pubsubpublisherconfig "github.com/maksimru/event-scheduler/publisher/pubsub/config"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
	"io"
)

type prioritizedFSM struct {
	storage *storage.PqStorage
}

func NewPrioritizedFSM(storage *storage.PqStorage) raft.FSM {
	return &prioritizedFSM{
		storage: storage,
	}
}

const OperationMessagePush int = 0
const OperationMessagePop int = 1
const OperationChannelCreate int = 2
const OperationChannelDelete int = 3
const OperationChannelUpdate int = 4

type CommandPayload struct {
	Operation int
	ChannelID string
	Message   message.Message
	Channel   channel.Channel
}

type ApplyResponse struct {
	Data interface{}
	Err  error
}

type fsmSnapshot struct {
	channelsDump []channel.Channel
	messagesDump map[string][]message.Message
}

type ChannelMessage struct {
	ChannelID string
	Message   message.Message
}

type Structs byte

const MessageStruct Structs = 1
const ChannelsStruct Structs = 2

func (f fsmSnapshot) persistChannels(sink raft.SnapshotSink, encoder *json.Encoder) error {
	for _, c := range f.channelsDump {
		sink.Write([]byte{byte(ChannelsStruct)})
		err := encoder.Encode(c)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f fsmSnapshot) persistMessages(sink raft.SnapshotSink, encoder *json.Encoder) error {
	for _, c := range f.channelsDump {
		for _, msg := range f.messagesDump[c.ID] {
			sink.Write([]byte{byte(MessageStruct)})
			err := encoder.Encode(&ChannelMessage{
				ChannelID: c.ID,
				Message:   msg,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (f fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {

		encoder := json.NewEncoder(sink)
		if err := f.persistChannels(sink, encoder); err != nil {
			return err
		}
		if err := f.persistMessages(sink, encoder); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		log.Errorf("error during snapshot persist %s\n", err.Error())
		_ = sink.Cancel()
		return err
	} else {
		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Release is invoked when we are finished with the snapshot.
func (f fsmSnapshot) Release() {
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (b prioritizedFSM) Apply(raftLog *raft.Log) interface{} {
	switch raftLog.Type {
	case raft.LogCommand:
		var payload = CommandPayload{}
		if err := json.Unmarshal(raftLog.Data, &payload); err != nil {
			log.Errorf("error marshalling store payload %s\n", err.Error())
			return nil
		}
		switch payload.Operation {
		case OperationMessagePush:
			s, has := b.storage.GetChannelStorage(payload.ChannelID)
			if has {
				s.Enqueue(payload.Message)
			}
			return &ApplyResponse{
				Data: payload.Message,
			}
		case OperationMessagePop:
			data := message.Message{}
			s, has := b.storage.GetChannelStorage(payload.ChannelID)
			if has {
				data = s.Dequeue()
			}
			return &ApplyResponse{
				Data: data,
			}
		case OperationChannelCreate:
			c, err := b.storage.AddChannel(remapChannelConfig(payload.Channel))
			return &ApplyResponse{
				Data: c,
				Err:  err,
			}
		case OperationChannelDelete:
			c, err := b.storage.DeleteChannel(payload.ChannelID)
			return &ApplyResponse{
				Data: c,
				Err:  err,
			}
		case OperationChannelUpdate:
			c, err := b.storage.UpdateChannel(payload.ChannelID, remapChannelConfig(payload.Channel))
			return &ApplyResponse{
				Data: c,
				Err:  err,
			}
		}
	}
	return nil
}

// transform hash map config to real objects
func remapChannelConfig(c channel.Channel) channel.Channel {
	switch c.Source.Driver {
	case "pubsub":
		var cfg pubsublistenerconfig.SourceConfig
		err := mapstructure.Decode(c.Source.Config, &cfg)
		if err != nil {
			panic(err)
		}
		c.Source.Config = cfg
	default:
		// truncate configs of unsupported drivers
		c.Source.Config = nil
	}
	switch c.Destination.Driver {
	case "pubsub":
		var cfg pubsubpublisherconfig.DestinationConfig
		err := mapstructure.Decode(c.Destination.Config, &cfg)
		if err != nil {
			panic(err)
		}
		c.Destination.Config = cfg
	default:
		// truncate configs of unsupported drivers
		c.Destination.Config = nil
	}
	return c
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (b prioritizedFSM) Snapshot() (raft.FSMSnapshot, error) {
	channelsDump, messagesDump := b.storage.Dump()
	return &fsmSnapshot{
		channelsDump: channelsDump,
		messagesDump: messagesDump,
	}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (b prioritizedFSM) Restore(rClose io.ReadCloser) error {
	defer func() {
		if rClose == nil {
			return
		}
		if err := rClose.Close(); err != nil {
			log.Errorf("Snapshot restore failed: close error %s\n", err.Error())
		}
	}()

	log.Infof("Snapshot restore started: read all channels and messages from the snapshot\n")
	var totalChannelsRestored int
	var totalMessagesRestored int

	b.storage.Flush()
	reader := bufio.NewReader(rClose)
	for {
		// Read the message type
		prefix, err := reader.ReadByte()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		structType := Structs(prefix)
		switch structType {
		case ChannelsStruct:
			var c channel.Channel
			data, _, _ := reader.ReadLine()
			err := json.Unmarshal(data, &c)
			if err != nil {
				log.Errorf("Snapshot restore failed: error decode channel data %s\n", err.Error())
				return err
			}
			b.storage.AddChannel(remapChannelConfig(c))
			totalChannelsRestored++
		case MessageStruct:
			var m ChannelMessage
			data, _, _ := reader.ReadLine()
			err := json.Unmarshal(data, &m)
			if err != nil {
				log.Errorf("Snapshot restore failed: error decode message data %s\n", err.Error())
				return err
			}
			s, has := b.storage.GetChannelStorage(m.ChannelID)
			if has {
				s.Enqueue(m.Message)
				totalMessagesRestored++
			} else {
				log.Error("Unable to find channel information inside the snapshot")
			}
		}
	}

	log.Infof("Snapshot restore finished: restored %d messages in %d channels in snapshot\n", totalMessagesRestored, totalChannelsRestored)
	return nil
}
