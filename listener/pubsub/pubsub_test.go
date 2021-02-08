package pubsub

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"context"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/fsm"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/prioritizer"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
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
		config      config.Config
		prioritizer *prioritizer.Prioritizer
	}
	type args struct {
		context     context.Context
		config      config.Config
		prioritizer *prioritizer.Prioritizer
	}
	dir := getProjectPath()
	cfg := config.Config{
		PubsubListenerProjectID: "testProjectId",
		PubsubListenerKeyFile:   dir + "/tests/pubsub_cred_mock.json",
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Check pubsub listener boot",
			fields: fields{
				config:      cfg,
				prioritizer: new(prioritizer.Prioritizer),
			},
			args: args{
				context:     context.Background(),
				config:      cfg,
				prioritizer: new(prioritizer.Prioritizer),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Listener{
				config:      tt.fields.config,
				prioritizer: tt.fields.prioritizer,
			}
			if !tt.wantErr {
				assert.NoError(t, l.Boot(tt.args.context, tt.args.config, tt.args.prioritizer))
			} else {
				assert.Error(t, l.Boot(tt.args.context, tt.args.config, tt.args.prioritizer))
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
		config config.Config
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
				config: config.Config{
					PubsubListenerProjectID: "testProjectId",
					PubsubListenerKeyFile:   dir + "/tests/pubsub_cred_mock.json",
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

func mockPubsubClient(ctx context.Context, t *testing.T, pubsubServerConn *grpc.ClientConn, cfg config.Config) (*pubsub.Client, *pubsub.Topic) {
	pubsubClient, _ := pubsub.NewClient(ctx, "", option.WithGRPCConn(pubsubServerConn))
	topic, err := pubsubClient.CreateTopic(ctx, cfg.PubsubListenerSubscriptionID)
	if err != nil {
		t.Error(err)
	}
	_, _ = pubsubClient.CreateSubscription(ctx, cfg.PubsubListenerSubscriptionID, pubsub.SubscriptionConfig{
		Topic: topic,
	})
	return pubsubClient, topic
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

func TestListenerPubsub_Listen(t *testing.T) {
	type fields struct {
	}
	pubsubServer := pstest.NewServer()
	defer func() {
		_ = pubsubServer.Close()
	}()

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
		publish []*pubsub.Message
		want    []message.Message
	}{
		{
			name:   "Check pubsub listener can receive single message with available_at attribute",
			fields: fields{},
			publish: []*pubsub.Message{{
				Data:       []byte("foo"),
				Attributes: map[string]string{"available_at": "1000"},
			}},
			wantErr: false,
			want:    []message.Message{message.NewMessage("foo", 1000)},
		},
		{
			name:   "Check pubsub listener can receive single message without available_at attribute",
			fields: fields{},
			publish: []*pubsub.Message{{
				Data:       []byte("foo"),
				Attributes: map[string]string{},
			}},
			wantErr: false,
			want:    []message.Message{},
		},
		{
			name:   "Check pubsub listener can receive single message with wrong available_at attribute",
			fields: fields{},
			publish: []*pubsub.Message{{
				Data:       []byte("foo"),
				Attributes: map[string]string{"available_at": "foo"},
			}},
			wantErr: false,
			want:    []message.Message{},
		},
		{
			name:   "Check pubsub listener can receive multiple messages",
			fields: fields{},
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
			wantErr: false,
			want: []message.Message{
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg2", 1100),
				message.NewMessage("msg3", 1200),
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
			cluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(nodeId),
					Address:  clusterAddr,
				},
			}})

			// wait for election
			time.Sleep(time.Second * 3)

			cfg := config.Config{
				PubsubListenerSubscriptionID: "mocksubscription" + strconv.Itoa(testID),
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
			}

			// publish test messages
			for _, msg := range tt.publish {
				pubsubServer.Publish(topic.String(), msg.Data, msg.Attributes)
			}
			if !tt.wantErr {
				assert.NoError(t, l.Listen())
			} else {
				assert.Error(t, l.Listen())
			}

			// read received messages
			var got []message.Message
			for !pqStorage.IsEmpty() {
				item := pqStorage.Dequeue()
				got = append(got, item)
			}
			// compare received messages
			if !reflect.DeepEqual(got, tt.want) {
				assert.ElementsMatch(t, tt.want, got)
			}
		})
	}
}
