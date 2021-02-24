package processor

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/dispatcher"
	"github.com/maksimru/event-scheduler/fsm"
	"github.com/maksimru/event-scheduler/message"
	pubsubconfig "github.com/maksimru/event-scheduler/publisher/config"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func TestProcessor_Boot(t *testing.T) {
	type fields struct {
		dispatcher  dispatcher.Dispatcher
		dataStorage *storage.PqStorage
		cluster     *raft.Raft
		channel     channel.Channel
	}
	type args struct {
		dispatcher  dispatcher.Dispatcher
		dataStorage *storage.PqStorage
		context     context.Context
		cluster     *raft.Raft
		channel     channel.Channel
	}
	dataStorage := storage.NewPqStorage()
	dispatcherProvider := dispatcher.NewDispatcher(context.Background(), goconcurrentqueue.NewFIFO(), dataStorage)
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Check processor boot",
			fields: fields{
				dispatcher:  dispatcherProvider,
				dataStorage: dataStorage,
				cluster:     &raft.Raft{},
				channel:     channel.Channel{ID: "ch1"},
			},
			args: args{
				dispatcher:  dispatcherProvider,
				dataStorage: dataStorage,
				context:     context.Background(),
				cluster:     &raft.Raft{},
				channel:     channel.Channel{ID: "ch1"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Processor{
				dispatcher:  tt.fields.dispatcher,
				dataStorage: tt.fields.dataStorage,
				cluster:     tt.fields.cluster,
			}
			if !tt.wantErr {
				assert.NoError(t, p.Boot(tt.args.context, tt.args.dispatcher, tt.args.dataStorage, tt.args.cluster, tt.args.channel))
			} else {
				assert.Error(t, p.Boot(tt.args.context, tt.args.dispatcher, tt.args.dataStorage, tt.args.cluster, tt.args.channel))
			}
		})
	}
}

func inmemConfig() *raft.Config {
	conf := raft.DefaultConfig()
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	return conf
}

func bootStagingCluster(nodeId string, pqStorage *storage.PqStorage) (*raft.Raft, raft.ServerAddress) {
	store := raft.NewInmemStore()
	cacheStore, _ := raft.NewLogCache(128, store)
	snapshotStore := raft.NewInmemSnapshotStore()
	raftTransportTcpAddr := raft.NewInmemAddr()
	_, transport := raft.NewInmemTransport(raftTransportTcpAddr)
	raftconfig := inmemConfig()
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
		dataStorage       *storage.PqStorage
		time              CurrentTimeChecker
		channel           channel.Channel
		availableChannels []channel.Channel
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
			name: "Check processor can move old messages to dispatch (as cluster leader)",
			fields: fields{
				dataStorage: storage.NewPqStorage(),
				time:        RealTime{},
				channel: channel.Channel{
					ID: "ch1",
					Destination: channel.Destination{
						Driver: "pubsub",
						Config: pubsubconfig.DestinationConfig{},
					},
				},
				availableChannels: []channel.Channel{
					{
						ID: "ch1",
						Destination: channel.Destination{
							Driver: "pubsub",
							Config: pubsubconfig.DestinationConfig{},
						},
					},
				},
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
			name: "Check processor doesn't move messages if they are not ready for dispatch (as cluster leader)",
			fields: fields{
				dataStorage: storage.NewPqStorage(),
				time:        NewMockTime(time.Unix(0, 0)), // simulate 0 timestamp
				channel: channel.Channel{
					ID: "ch1",
					Destination: channel.Destination{
						Driver: "pubsub",
						Config: pubsubconfig.DestinationConfig{},
					},
				},
				availableChannels: []channel.Channel{
					{
						ID: "ch1",
						Destination: channel.Destination{
							Driver: "pubsub",
							Config: pubsubconfig.DestinationConfig{},
						},
					},
				},
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
			name: "Check processor can partially dispatch prepared messages on time (as cluster leader)",
			fields: fields{
				dataStorage: storage.NewPqStorage(),
				time:        NewMockTime(time.Unix(600, 0)), // simulate 600 timestamp
				channel: channel.Channel{
					ID: "ch1",
					Destination: channel.Destination{
						Driver: "pubsub",
						Config: pubsubconfig.DestinationConfig{},
					},
				},
				availableChannels: []channel.Channel{
					{
						ID: "ch1",
						Destination: channel.Destination{
							Driver: "pubsub",
							Config: pubsubconfig.DestinationConfig{},
						},
					},
				},
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
			name: "Check processor can't move old messages to dispatch (as cluster slave)",
			fields: fields{
				dataStorage: storage.NewPqStorage(),
				time:        RealTime{},
				channel: channel.Channel{
					ID: "ch1",
					Destination: channel.Destination{
						Driver: "pubsub",
						Config: pubsubconfig.DestinationConfig{},
					},
				},
				availableChannels: []channel.Channel{
					{
						ID: "ch1",
						Destination: channel.Destination{
							Driver: "pubsub",
							Config: pubsubconfig.DestinationConfig{},
						},
					},
				},
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
			dispatcherInstance := dispatcher.NewDispatcher(ctx, outboundQueue, tt.fields.dataStorage)

			nodeId := string(rune(testID))
			cluster, clusterAddr := bootStagingCluster(nodeId, tt.fields.dataStorage)
			defer func() {
				_ = cluster.Shutdown()
			}()

			p := &Processor{
				dispatcher:  dispatcherInstance,
				dataStorage: tt.fields.dataStorage,
				time:        tt.fields.time,
				context:     ctx,
				cluster:     cluster,
				channel:     tt.fields.channel,
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
			for _, c := range tt.fields.availableChannels {
				_, _ = p.dataStorage.AddChannel(c)
			}

			// wait for election
			time.Sleep(time.Second * 1)

			chStorage, _ := p.dataStorage.GetChannelStorage(tt.fields.channel.ID)
			for _, msg := range tt.storageData {
				chStorage.Enqueue(msg)
			}

			// execute prioritizer
			if !tt.wantErr {
				assert.NoError(t, p.Process())
			} else {
				assert.Error(t, p.Process())
			}

			// validate unprocessed records
			gotStorage := []message.Message{}
			for !chStorage.IsEmpty() {
				gotStorage = append(gotStorage, chStorage.Dequeue())
			}
			if !reflect.DeepEqual(gotStorage, tt.wantStorage) {
				assert.Equal(t, tt.wantStorage, gotStorage)
			}
			// validate results
			gotPublished := []message.Message{}
			for outboundQueue.GetLen() > 0 {
				msg, _ := outboundQueue.Dequeue()
				delivery := msg.(dispatcher.MessageForDelivery)
				gotPublished = append(gotPublished, delivery.GetMessage())
			}
			if !reflect.DeepEqual(gotPublished, tt.wantPublished) {
				assert.Equal(t, tt.wantPublished, gotPublished)
			}
		})
	}
}
