package prioritizer

import (
	"context"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/fsm"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func TestPrioritizer_Boot(t *testing.T) {
	type fields struct {
		cluster *raft.Raft
	}
	type args struct {
		cluster *raft.Raft
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Check prioritizer boot",
			fields: fields{
				cluster: &raft.Raft{},
			},
			args: args{
				cluster: &raft.Raft{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Prioritizer{
				cluster: tt.fields.cluster,
			}
			if !tt.wantErr {
				assert.NoError(t, p.Boot(tt.args.cluster))
			} else {
				assert.Error(t, p.Boot(tt.args.cluster))
			}
		})
	}
}

func bootStagingCluster(nodeId string, pqStorage *storage.PqStorage) (*raft.Raft, raft.ServerAddress) {
	store := raft.NewInmemStore()
	cacheStore, _ := raft.NewLogCache(128, store)
	snapshotStore := raft.NewInmemSnapshotStore()
	raftTransportTcpAddr := raft.NewInmemAddr()
	_, transport := raft.NewInmemTransport(raftTransportTcpAddr)
	raftconfig := raft.DefaultConfig()
	raftconfig.LogLevel = "info"
	raftconfig.LocalID = raft.ServerID(nodeId)
	raftconfig.SnapshotThreshold = 512
	raftServer, err := raft.NewRaft(raftconfig, fsm.NewPrioritizedFSM(pqStorage), cacheStore, store, snapshotStore, transport)
	if err != nil {
		panic("exception during staging cluster boot: " + err.Error())
	}
	return raftServer, raftTransportTcpAddr
}

func TestPrioritizer_Process(t *testing.T) {
	type fields struct {
	}
	tests := []struct {
		name        string
		fields      fields
		wantErr     bool
		inboundMsgs []message.Message
		want        []message.Message
	}{
		{
			name:   "Check prioritizer can persist one message to the storage",
			fields: fields{},
			inboundMsgs: []message.Message{
				message.NewMessage("msg1", 1000),
			},
			want: []message.Message{
				message.NewMessage("msg1", 1000),
			},
			wantErr: false,
		},
		{
			name:   "Check prioritizer can persist more than single message with right priority",
			fields: fields{},
			inboundMsgs: []message.Message{
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
				message.NewMessage("msg4", 2000),
				message.NewMessage("msg5", 1200),
			},
			want: []message.Message{
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg5", 1200),
				message.NewMessage("msg4", 2000),
			},
			wantErr: false,
		},
	}
	for testID, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()

			pqStorage := storage.NewPqStorage()
			nodeId := string(rune(testID))
			cluster, clusterAddr := bootStagingCluster(nodeId, pqStorage)
			defer func() {
				_ = cluster.Shutdown()
			}()

			p := &Prioritizer{
				cluster: cluster,
			}

			// boot required cluster
			p.cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(nodeId),
					Address:  clusterAddr,
				},
			}})

			// wait for election
			time.Sleep(time.Second * 3)

			// insert requested input
			for _, msg := range tt.inboundMsgs {
				_ = p.Persist(msg)
			}

			// validate results
			var got []message.Message
			for !pqStorage.IsEmpty() {
				got = append(got, pqStorage.Dequeue())
			}

			if !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
