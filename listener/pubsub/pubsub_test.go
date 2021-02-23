package pubsub

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"context"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/fsm"
	pubsubconfig "github.com/maksimru/event-scheduler/listener/config"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/prioritizer"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"path"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestListenerPubsub_Boot(t *testing.T) {
	type fields struct {
		context     context.Context
		prioritizer *prioritizer.Prioritizer
		channel     channel.Channel
	}
	type args struct {
		context     context.Context
		config      config.Config
		prioritizer *prioritizer.Prioritizer
		channel     channel.Channel
	}
	dir := getProjectPath()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Check pubsub listener boot",
			fields: fields{
				prioritizer: new(prioritizer.Prioritizer),
				channel: channel.Channel{
					Source: channel.Source{
						Driver: "pubsub",
						Config: pubsubconfig.SourceConfig{
							ProjectID:      "pr",
							SubscriptionID: "sub",
							KeyFile:        dir + "/tests/pubsub_cred_mock.json",
						},
					},
				},
				context: context.Background(),
			},
			args: args{
				context:     context.Background(),
				prioritizer: new(prioritizer.Prioritizer),
				channel: channel.Channel{
					Source: channel.Source{
						Driver: "pubsub",
						Config: pubsubconfig.SourceConfig{
							ProjectID:      "pr",
							SubscriptionID: "sub",
							KeyFile:        dir + "/tests/pubsub_cred_mock.json",
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Listener{
				context:     tt.fields.context,
				prioritizer: tt.fields.prioritizer,
				channel:     tt.fields.channel,
			}
			if !tt.wantErr {
				assert.NoError(t, l.Boot(tt.args.context, tt.args.channel, tt.args.prioritizer))
			} else {
				assert.Error(t, l.Boot(tt.args.context, tt.args.channel, tt.args.prioritizer))
			}
		})
	}
}

func getProjectPath() string {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "../..")
	return dir
}

func Test_makePubsubClient(t *testing.T) {
	type args struct {
		ctx    context.Context
		config pubsubconfig.SourceConfig
	}
	dir := getProjectPath()
	tests := []struct {
		name    string
		args    args
		want    *pubsub.Client
		wantErr bool
	}{
		{
			name: "Check make pubsub client with proper configuration",
			args: args{
				ctx: context.Background(),
				config: pubsubconfig.SourceConfig{
					ProjectID: "testProjectId",
					KeyFile:   dir + "/tests/pubsub_cred_mock.json",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := makePubsubClient(tt.args.ctx, tt.args.config)
			defer func() {
				_ = client.Close()
			}()
			if (err != nil) != tt.wantErr {
				t.Errorf("makePubsubClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func mockPubsubClient(ctx context.Context, t *testing.T, pubsubServerConn *grpc.ClientConn, cfg pubsubconfig.SourceConfig) (*pubsub.Client, *pubsub.Topic) {
	pubsubClient, _ := pubsub.NewClient(ctx, "", option.WithGRPCConn(pubsubServerConn))
	topic, err := pubsubClient.CreateTopic(ctx, cfg.SubscriptionID)
	if err != nil {
		t.Error(err)
	}
	_, _ = pubsubClient.CreateSubscription(ctx, cfg.SubscriptionID, pubsub.SubscriptionConfig{
		Topic: topic,
	})
	return pubsubClient, topic
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

func TestListenerPubsub_Listen(t *testing.T) {
	type fields struct {
		channel           channel.Channel
		availableChannels []channel.Channel
	}
	pubsubServer := pstest.NewServer()
	defer func() {
		_ = pubsubServer.Close()
	}()

	tests := []struct {
		name         string
		fields       fields
		wantErr      bool
		publish      []*pubsub.Message
		publishDelay []*pubsub.Message
		want         []message.Message
	}{
		{
			name: "Check pubsub listener can receive single message with available_at attribute",
			fields: fields{
				channel: channel.Channel{
					ID: "ch1",
					Source: channel.Source{
						Driver: "pubsub",
					},
				},
				availableChannels: []channel.Channel{
					{
						ID: "ch1",
					},
				},
			},
			publish: []*pubsub.Message{{
				Data:       []byte("foo"),
				Attributes: map[string]string{"available_at": "1000"},
			}},
			publishDelay: nil,
			wantErr:      false,
			want:         []message.Message{message.NewMessage("foo", 1000)},
		},
		{
			name: "Check pubsub listener can receive single message without available_at attribute",
			fields: fields{
				channel: channel.Channel{
					ID: "ch1",
					Source: channel.Source{
						Driver: "pubsub",
					},
				},
				availableChannels: []channel.Channel{
					{
						ID: "ch1",
					},
				},
			},
			publish: []*pubsub.Message{{
				Data:       []byte("foo"),
				Attributes: map[string]string{},
			}},
			publishDelay: nil,
			wantErr:      false,
			want:         []message.Message{},
		},
		{
			name: "Check pubsub listener can receive single message with wrong available_at attribute",
			fields: fields{
				channel: channel.Channel{
					ID: "ch1",
					Source: channel.Source{
						Driver: "pubsub",
					},
				},
				availableChannels: []channel.Channel{
					{
						ID: "ch1",
					},
				},
			},
			publish: []*pubsub.Message{{
				Data:       []byte("foo"),
				Attributes: map[string]string{"available_at": "foo"},
			}},
			publishDelay: nil,
			wantErr:      false,
			want:         []message.Message{},
		},
		{
			name: "Check pubsub listener can receive multiple messages",
			fields: fields{
				channel: channel.Channel{
					ID: "ch1",
					Source: channel.Source{
						Driver: "pubsub",
					},
				},
				availableChannels: []channel.Channel{
					{
						ID: "ch1",
					},
				},
			},
			publish: []*pubsub.Message{{
				Data:       []byte("msg1"),
				Attributes: map[string]string{"available_at": "1000"},
			}, {
				Data:       []byte("msg2"),
				Attributes: map[string]string{"available_at": "1100"},
			}, {
				Data:       []byte("msg3"),
				Attributes: map[string]string{"available_at": "1200"},
			}},
			publishDelay: nil,
			wantErr:      false,
			want: []message.Message{
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg2", 1100),
				message.NewMessage("msg3", 1200),
			},
		},
		{
			name: "Check pubsub listener can receive multiple messages with publish delay",
			fields: fields{
				channel: channel.Channel{
					ID: "ch1",
					Source: channel.Source{
						Driver: "pubsub",
					},
				},
				availableChannels: []channel.Channel{
					{
						ID: "ch1",
					},
				},
			},
			publish: []*pubsub.Message{{
				Data:       []byte("msg1"),
				Attributes: map[string]string{"available_at": "1000"},
			}, {
				Data:       []byte("msg2"),
				Attributes: map[string]string{"available_at": "1100"},
			}, {
				Data:       []byte("msg3"),
				Attributes: map[string]string{"available_at": "1200"},
			}},
			publishDelay: []*pubsub.Message{{
				Data:       []byte("msg6"),
				Attributes: map[string]string{"available_at": "1500"},
			}, {
				Data:       []byte("msg5"),
				Attributes: map[string]string{"available_at": "1400"},
			}, {
				Data:       []byte("msg4"),
				Attributes: map[string]string{"available_at": "1300"},
			}},
			wantErr: false,
			want: []message.Message{
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg2", 1100),
				message.NewMessage("msg3", 1200),
				message.NewMessage("msg4", 1300),
				message.NewMessage("msg5", 1400),
				message.NewMessage("msg6", 1500),
			},
		},
	}
	for testID, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// mock individual context for each test
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()

			pqStorage := storage.NewPqStorage()
			nodeId := string(rune(testID))
			cluster, clusterAddr := bootStagingCluster(nodeId, pqStorage)
			defer func() {
				_ = cluster.Shutdown()
			}()

			p := new(prioritizer.Prioritizer)
			_ = p.Boot(cluster)

			// boot required cluster
			f := cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(nodeId),
					Address:  clusterAddr,
				},
			}})

			err := f.Error()
			if err != nil {
				t.Fatal("Cluster bootstrap failed: ", err)
			}

			// wait for election
			time.Sleep(time.Second * 1)

			for _, c := range tt.fields.availableChannels {
				_, _ = pqStorage.AddChannel(c)
			}

			cfg := pubsubconfig.SourceConfig{
				SubscriptionID: "mocksubscription" + strconv.Itoa(testID),
			}

			// make pubsub client-server connection
			pubsubServerConn, _ := grpc.Dial(pubsubServer.Addr, grpc.WithInsecure())
			defer func() {
				_ = pubsubServerConn.Close()
			}()
			pubsubClient, topic := mockPubsubClient(ctx, t, pubsubServerConn, cfg)

			l := &Listener{
				config:      cfg,
				client:      pubsubClient,
				context:     ctx,
				prioritizer: p,
				channel:     tt.fields.channel,
			}

			// publish test messages
			for _, msg := range tt.publish {
				pubsubServer.Publish(topic.String(), msg.Data, msg.Attributes)
			}

			g, ctx := errgroup.WithContext(ctx)

			// run listener in background
			g.Go(func() error {
				if !tt.wantErr {
					assert.NoError(t, l.Listen())
				} else {
					assert.Error(t, l.Listen())
				}
				return nil
			})

			// simulate second batch
			if tt.publishDelay != nil {
				g.Go(func() error {
					time.Sleep(time.Second * 2)
					// publish second batch of messages
					for _, msg := range tt.publishDelay {
						pubsubServer.Publish(topic.String(), msg.Data, msg.Attributes)
					}
					return nil
				})
			}
			g.Wait()

			// read received messages
			chStorage, _ := pqStorage.GetChannelStorage(tt.fields.channel.ID)
			var got []message.Message
			for !chStorage.IsEmpty() {
				item := chStorage.Dequeue()
				got = append(got, item)
			}
			// compare received messages
			if !reflect.DeepEqual(got, tt.want) {
				assert.ElementsMatch(t, tt.want, got)
			}
		})
	}
}
