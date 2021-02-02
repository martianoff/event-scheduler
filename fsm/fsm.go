package fsm

import (
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/storage"
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

const OperationPush int = 0
const OperationPop int = 1

type CommandPayload struct {
	Operation int
	Value     message.Message
}

type ApplyResponse struct {
	Data interface{}
}

type fsmSnapshot struct {
	dump *[]message.Message
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (f fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {

		b, err := json.Marshal(f.dump)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
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
		case OperationPush:
			b.storage.Enqueue(payload.Value)
			return &ApplyResponse{
				Data: payload.Value,
			}
		case OperationPop:
			return &ApplyResponse{
				Data: b.storage.Dequeue(),
			}
		}
	}
	return nil
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (b prioritizedFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{dump: b.storage.Dump()}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (b prioritizedFSM) Restore(rClose io.ReadCloser) error {
	defer func() {
		if err := rClose.Close(); err != nil {
			log.Errorf("Snapshot restore failed: close error %s\n", err.Error())
		}
	}()

	log.Infof("Snapshot restore started: read all messages from the snapshot\n")
	var totalRestored int

	b.storage.Flush()
	decoder := json.NewDecoder(rClose)
	for decoder.More() {
		var data = message.Message{}
		err := decoder.Decode(&data)
		if err != nil {
			log.Errorf("Snapshot restore failed: error decode data %s\n", err.Error())
			return err
		}
		b.storage.Enqueue(data)
		totalRestored++
	}

	// read closing bracket
	_, err := decoder.Token()
	if err != nil {
		log.Errorf("Snapshot restore failed: error %s\n", err.Error())
		return err
	}

	log.Infof("Snapshot restore finished: success restore %d messages in snapshot\n", totalRestored)
	return nil
}
