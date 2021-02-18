package channelmanager

import (
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/fsm"
	pubsublistenerconfig "github.com/maksimru/event-scheduler/listener/config"
	"github.com/maksimru/event-scheduler/nodenameresolver"
	pubsubpublisherconfig "github.com/maksimru/event-scheduler/publisher/config"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSchedulerChannelManager_BootChannelManager(t *testing.T) {
	type fields struct {
		cluster *raft.Raft
		storage *storage.PqStorage
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Check manager boot",
			fields: fields{
				cluster: &raft.Raft{},
				storage: storage.NewPqStorage(),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &SchedulerChannelManager{}
			if !tt.wantErr {
				assert.NoError(t, m.BootChannelManager(tt.fields.cluster, tt.fields.storage))
			} else {
				assert.Error(t, m.BootChannelManager(tt.fields.cluster, tt.fields.storage))
			}
			assert.Equal(t, tt.fields.cluster, m.cluster)
			assert.Equal(t, tt.fields.storage, m.storage)
		})
	}
}

func TestSchedulerChannelManager_GetChannels(t *testing.T) {
	type fields struct {
		cluster *raft.Raft
	}
	tests := []struct {
		name     string
		fields   fields
		channels []channel.Channel
		want     []channel.Channel
		wantErr  bool
	}{
		{
			name: "Check get channels",
			fields: fields{
				cluster: &raft.Raft{},
			},
			channels: []channel.Channel{
				{
					ID:          "1",
					Source:      channel.Source{},
					Destination: channel.Destination{},
				},
				{
					ID:          "2",
					Source:      channel.Source{},
					Destination: channel.Destination{},
				},
			},
			want: []channel.Channel{
				{
					ID:          "1",
					Source:      channel.Source{},
					Destination: channel.Destination{},
				},
				{
					ID:          "2",
					Source:      channel.Source{},
					Destination: channel.Destination{},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			cluster, clusterTransport, pqStorage := bootStagingCluster()
			defer func() {
				_ = cluster.Shutdown()
			}()

			// boot required cluster
			cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       nodenameresolver.Resolve(string(clusterTransport.LocalAddr())),
					Address:  clusterTransport.LocalAddr(),
				},
			}})

			// wait for election
			time.Sleep(time.Second * 1)

			for _, c := range tt.channels {
				_, _ = pqStorage.AddChannel(c)
			}
			sc := &SchedulerChannelManager{
				cluster: cluster,
				storage: pqStorage,
			}
			got, err := sc.GetChannels()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.ElementsMatch(t, tt.want, got)
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

func bootStagingCluster() (*raft.Raft, *raft.InmemTransport, *storage.PqStorage) {
	store := raft.NewInmemStore()
	cacheStore, _ := raft.NewLogCache(128, store)
	snapshotStore := raft.NewInmemSnapshotStore()
	_, transport := raft.NewInmemTransport("")
	raftconfig := inmemConfig()
	raftconfig.LogLevel = "info"
	raftconfig.LocalID = nodenameresolver.Resolve(string(transport.LocalAddr()))
	raftconfig.SnapshotThreshold = 512
	dataStorage := storage.NewPqStorage()
	raftServer, err := raft.NewRaft(raftconfig, fsm.NewPrioritizedFSM(dataStorage), cacheStore, store, snapshotStore, transport)
	if err != nil {
		panic("exception during staging cluster boot: " + err.Error())
	}
	return raftServer, transport, dataStorage
}

func TestSchedulerChannelManager_AddChannel(t *testing.T) {
	type args struct {
		channelInput channel.Channel
	}
	tests := []struct {
		name    string
		args    args
		want    channel.Channel
		wantErr bool
	}{
		{
			name: "Check add channel with specific id and correct input",
			args: args{
				channelInput: channel.Channel{
					ID:          "1",
					Source:      channel.Source{},
					Destination: channel.Destination{},
				},
			},
			want: channel.Channel{
				ID:          "1",
				Source:      channel.Source{},
				Destination: channel.Destination{},
			},
			wantErr: false,
		},
		{
			name: "Check add channel without specific id and correct input",
			args: args{
				channelInput: channel.Channel{
					Source:      channel.Source{},
					Destination: channel.Destination{},
				},
			},
			want: channel.Channel{
				Source:      channel.Source{},
				Destination: channel.Destination{},
			},
			wantErr: false,
		},
		{
			name: "Check add channel with all options",
			args: args{
				channelInput: channel.Channel{
					ID: "1",
					Source: channel.Source{
						Driver: "pubsub",
						Config: pubsublistenerconfig.SourceConfig{
							ProjectID:      "test",
							SubscriptionID: "sub",
							KeyFile:        "key",
						},
					},
					Destination: channel.Destination{
						Driver: "pubsub",
						Config: pubsubpublisherconfig.DestinationConfig{
							ProjectID: "test",
							TopicID:   "topic",
							KeyFile:   "key",
						},
					},
				},
			},
			want: channel.Channel{
				ID: "1",
				Source: channel.Source{
					Driver: "pubsub",
					Config: pubsublistenerconfig.SourceConfig{
						ProjectID:      "test",
						SubscriptionID: "sub",
						KeyFile:        "key",
					},
				},
				Destination: channel.Destination{
					Driver: "pubsub",
					Config: pubsubpublisherconfig.DestinationConfig{
						ProjectID: "test",
						TopicID:   "topic",
						KeyFile:   "key",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			cluster, clusterTransport, pqStorage := bootStagingCluster()
			defer func() {
				_ = cluster.Shutdown()
			}()

			// boot required cluster
			cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       nodenameresolver.Resolve(string(clusterTransport.LocalAddr())),
					Address:  clusterTransport.LocalAddr(),
				},
			}})

			// wait for election
			time.Sleep(time.Second * 1)

			m := &SchedulerChannelManager{
				cluster: cluster,
				storage: pqStorage,
			}
			got, err := m.AddChannel(tt.args.channelInput)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tt.want.ID == "" {
				assert.NotEmpty(t, got.ID)
				assert.Equal(t, tt.want.Source, got.Source)
				assert.Equal(t, tt.want.Destination, got.Destination)
			} else {
				assert.Equal(t, tt.want, *got)
			}
		})
	}
}

func TestSchedulerChannelManager_DeleteChannel(t *testing.T) {
	type args struct {
		ID string
	}
	tests := []struct {
		name         string
		channels     []channel.Channel
		args         args
		wantErr      bool
		shouldExists bool
	}{
		{
			name: "Check delete channel with specific id",
			args: args{
				ID: "1",
			},
			channels: []channel.Channel{
				{
					ID:          "1",
					Source:      channel.Source{},
					Destination: channel.Destination{},
				},
			},
			wantErr:      false,
			shouldExists: true,
		},
		{
			name: "Check delete non existing channel with specific id",
			args: args{
				ID: "2",
			},
			channels: []channel.Channel{
				{
					ID:          "1",
					Source:      channel.Source{},
					Destination: channel.Destination{},
				},
			},
			wantErr:      true,
			shouldExists: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster, clusterTransport, pqStorage := bootStagingCluster()
			defer func() {
				_ = cluster.Shutdown()
			}()

			// boot required cluster
			cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       nodenameresolver.Resolve(string(clusterTransport.LocalAddr())),
					Address:  clusterTransport.LocalAddr(),
				},
			}})

			// wait for election
			time.Sleep(time.Second * 1)

			m := &SchedulerChannelManager{
				cluster: cluster,
				storage: pqStorage,
			}

			for _, c := range tt.channels {
				_, _ = pqStorage.AddChannel(c)
			}

			if tt.shouldExists {
				_, err := pqStorage.GetChannel(tt.args.ID)
				assert.NotEqual(t, err, storage.ErrChannelNotFound)
			}

			err := m.DeleteChannel(tt.args.ID)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			_, err = pqStorage.GetChannel(tt.args.ID)
			assert.Equal(t, err, storage.ErrChannelNotFound)

		})
	}
}

func TestSchedulerChannelManager_UpdateChannel(t *testing.T) {
	type fields struct {
		cluster *raft.Raft
		storage *storage.PqStorage
	}
	type args struct {
		ID           string
		channelInput channel.Channel
	}
	tests := []struct {
		name     string
		channels []channel.Channel
		fields   fields
		args     args
		want     *channel.Channel
		wantErr  bool
	}{
		{
			name: "Check update channel with specific id",
			args: args{
				ID: "1",
				channelInput: channel.Channel{
					Source: channel.Source{
						Driver: "test",
						Config: nil,
					},
					Destination: channel.Destination{
						Driver: "test",
						Config: nil,
					},
				},
			},
			channels: []channel.Channel{
				{
					ID: "1",
					Source: channel.Source{
						Driver: "pubsub",
						Config: nil,
					},
					Destination: channel.Destination{
						Driver: "pubsub",
						Config: nil,
					},
				},
			},
			want: &channel.Channel{
				ID: "1",
				Source: channel.Source{
					Driver: "test",
					Config: nil,
				},
				Destination: channel.Destination{
					Driver: "test",
					Config: nil,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster, clusterTransport, pqStorage := bootStagingCluster()
			defer func() {
				_ = cluster.Shutdown()
			}()

			// boot required cluster
			cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       nodenameresolver.Resolve(string(clusterTransport.LocalAddr())),
					Address:  clusterTransport.LocalAddr(),
				},
			}})

			// wait for election
			time.Sleep(time.Second * 1)

			m := &SchedulerChannelManager{
				cluster: cluster,
				storage: pqStorage,
			}

			for _, c := range tt.channels {
				_, _ = pqStorage.AddChannel(c)
			}

			c, err := m.UpdateChannel(tt.args.ID, tt.args.channelInput)

			assert.Equal(t, tt.want, c)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

		})
	}
}
