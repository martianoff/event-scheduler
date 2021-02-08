package processor

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/fsm"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/publisher"
	"github.com/maksimru/event-scheduler/publisher/pubsub"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func TestProcessor_Boot(t *testing.T) {
	type fields struct {
		publisher   publisher.Publisher
		dataStorage *storage.PqStorage
		cluster     *raft.Raft
	}
	type args struct {
		publisher   publisher.Publisher
		dataStorage *storage.PqStorage
		context     context.Context
		cluster     *raft.Raft
	}
	publisherProvider := new(pubsub.Publisher)
	dataStorage := storage.NewPqStorage()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Check processor boot",
			fields: fields{
				publisher:   publisherProvider,
				dataStorage: dataStorage,
				cluster:     &raft.Raft{},
			},
			args: args{
				publisher:   publisherProvider,
				dataStorage: dataStorage,
				context:     context.Background(),
				cluster:     &raft.Raft{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Processor{
				publisher:   tt.fields.publisher,
				dataStorage: tt.fields.dataStorage,
				cluster:     tt.fields.cluster,
			}
			if !tt.wantErr {
				assert.NoError(t, p.Boot(tt.args.context, tt.args.publisher, tt.args.dataStorage, tt.args.cluster))
			} else {
				assert.Error(t, p.Boot(tt.args.context, tt.args.publisher, tt.args.dataStorage, tt.args.cluster))
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

func TestProcessor_Process(t *testing.T) {
	type fields struct {
		dataStorage *storage.PqStorage
		time        CurrentTimeChecker
	}

	tests := []struct {
		name          string
		fields        fields
		wantErr       bool
		now           time.Time
		node          raft.ServerSuffrage
		storageData   []message.Message
		wantStorage   []message.Message
		wantPublished []message.Message
	}{
		{
			name: "Check prioritizer can move old messages to dispatch (as cluster leader)",
			fields: fields{
				dataStorage: storage.NewPqStorage(),
				time:        RealTime{},
			},
			storageData: []message.Message{
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg5", 1200),
				message.NewMessage("msg4", 2000),
			},
			node:        raft.Voter,
			wantStorage: []message.Message{},
			wantPublished: []message.Message{
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg5", 1200),
				message.NewMessage("msg4", 2000),
			},
			wantErr: false,
		},
		{
			name: "Check prioritizer don't move messages if they are not ready for dispatch (as cluster leader)",
			fields: fields{
				dataStorage: storage.NewPqStorage(),
				time:        NewMockTime(time.Unix(0, 0)), // simulate 0 timestamp
			},
			storageData: []message.Message{
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg5", 1200),
				message.NewMessage("msg4", 2000),
			},
			node: raft.Voter,
			wantStorage: []message.Message{
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg5", 1200),
				message.NewMessage("msg4", 2000),
			},
			wantPublished: []message.Message{},
			wantErr:       false,
		},
		{
			name: "Check prioritizer can partially dispatch prepared messages on time (as cluster leader)",
			fields: fields{
				dataStorage: storage.NewPqStorage(),
				time:        NewMockTime(time.Unix(600, 0)), // simulate 600 timestamp
			},
			storageData: []message.Message{
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg5", 1200),
				message.NewMessage("msg4", 2000),
			},
			node: raft.Voter,
			wantStorage: []message.Message{
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg5", 1200),
				message.NewMessage("msg4", 2000),
			},
			wantPublished: []message.Message{
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
			},
			wantErr: false,
		},
		{
			name: "Check prioritizer can't move old messages to dispatch (as cluster slave)",
			fields: fields{
				dataStorage: storage.NewPqStorage(),
				time:        RealTime{},
			},
			storageData: []message.Message{
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
			},
			node: raft.Nonvoter,
			wantStorage: []message.Message{
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
			},
			wantPublished: []message.Message{},
			wantErr:       false,
		},
	}
	for testID, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*4)
			defer cancel()

			outboundQueue := goconcurrentqueue.NewFIFO()
			publisherInstance := new(pubsub.Publisher)
			_ = publisherInstance.Boot(ctx, config.Config{}, outboundQueue)

			nodeId := string(rune(testID))
			cluster, clusterAddr := bootStagingCluster(nodeId, tt.fields.dataStorage)
			defer func() {
				_ = cluster.Shutdown()
			}()

			p := &Processor{
				publisher:   publisherInstance,
				dataStorage: tt.fields.dataStorage,
				context:     ctx,
				time:        tt.fields.time,
				cluster:     cluster,
			}

			// boot required cluster
			p.cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: tt.node,
					ID:       raft.ServerID(nodeId),
					Address:  clusterAddr,
				},
			}})

			// insert requested input
			for _, msg := range tt.storageData {
				p.dataStorage.Enqueue(msg)
			}

			// execute prioritizer
			if !tt.wantErr {
				assert.NoError(t, p.Process())
			} else {
				assert.Error(t, p.Process())
			}
			// validate unprocessed records
			gotStorage := []message.Message{}
			for !p.dataStorage.IsEmpty() {
				gotStorage = append(gotStorage, p.dataStorage.Dequeue())
			}
			if !reflect.DeepEqual(gotStorage, tt.wantStorage) {
				assert.Equal(t, tt.wantStorage, gotStorage)
			}
			// validate results
			gotPublished := []message.Message{}
			for outboundQueue.GetLen() > 0 {
				msg, _ := outboundQueue.Dequeue()
				gotPublished = append(gotPublished, msg.(message.Message))
			}
			if !reflect.DeepEqual(gotPublished, tt.wantPublished) {
				assert.Equal(t, tt.wantPublished, gotPublished)
			}
		})
	}
}
