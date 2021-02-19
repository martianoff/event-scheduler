package fsm

import (
	"bytes"
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/channel"
	pubsublistenerconfig "github.com/maksimru/event-scheduler/listener/config"
	"github.com/maksimru/event-scheduler/message"
	pubsubpublisherconfig "github.com/maksimru/event-scheduler/publisher/config"
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

const MSG_BATCH_SIZE int = 1000

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (f fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {

		buf := new(bytes.Buffer)
		encoder := json.NewEncoder(buf)
		channelBuf := new(bytes.Buffer)
		channelEncoder := json.NewEncoder(channelBuf)

		for _, c := range f.channelsDump {
			// persist channels
			err := encoder.Encode(c)
			if err != nil {
				return err
			}
			// write data to sink.
			if _, err := sink.Write(buf.Bytes()); err != nil {
				return err
			}
			buf.Reset()

			// persist messages
			msgIdx := 0
			for _, msg := range f.messagesDump[c.ID] {
				err := channelEncoder.Encode(msg)
				if err != nil {
					return err
				}
				msgIdx++
				if msgIdx%MSG_BATCH_SIZE == 0 {
					// write data to sink.
					if _, err := sink.Write(channelBuf.Bytes()); err != nil {
						return err
					}
					channelBuf.Reset()
				}
			}
			// write remaining messages
			if _, err := sink.Write(channelBuf.Bytes()); err != nil {
				return err
			}
			channelBuf.Reset()
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		_ = sink.Cancel()
		return err
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
	// using two pointers we can jump between two data structures
	lastPos := json.NewDecoder(rClose)
	decoder := json.NewDecoder(rClose)
	var lastChannel channel.Channel
	for decoder.More() {
		var c channel.Channel
		err := decoder.Decode(&c)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("Snapshot restore failed: error decode data %s\n", err.Error())
			return err
		}
		if c.ID != "" {
			lastChannel, _ = b.storage.AddChannel(c)
			totalChannelsRestored++
			*lastPos = *decoder
		} else {
			//jump to last correct position
			*decoder = *lastPos
			var m message.Message
			err := decoder.Decode(&m)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Errorf("Snapshot restore failed: error decode data %s\n", err.Error())
				return err
			}
			*lastPos = *decoder
			s, has := b.storage.GetChannelStorage(lastChannel.ID)
			if has {
				s.Enqueue(m)
				totalMessagesRestored++
			} else {
				log.Error("Unable to find channel information inside the snapshot")
			}
		}
	}

	log.Infof("Snapshot restore finished: restored %d messages in %d channels in snapshot\n", totalMessagesRestored, totalChannelsRestored)
	return nil
}
