package scheduler

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/listener"
	listenerpubsub "github.com/maksimru/event-scheduler/listener/pubsub"
	listenertest "github.com/maksimru/event-scheduler/listener/test"
	"github.com/maksimru/event-scheduler/prioritizer"
	"github.com/maksimru/event-scheduler/processor"
	"github.com/maksimru/event-scheduler/publisher"
	publisherpubsub "github.com/maksimru/event-scheduler/publisher/pubsub"
	publishertest "github.com/maksimru/event-scheduler/publisher/test"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"os"
	"path"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestNewScheduler(t *testing.T) {
	type args struct {
		config config.Config
	}
	tests := []struct {
		name string
		args args
		want *Scheduler
	}{
		{
			name: "Test scheduler constructor",
			args: args{
				config: config.Config{
					ListenerDriver:   "test",
					PublisherDriver:  "test",
					StoragePath:      getProjectPath() + "/tests/tempStorageNs1",
					ClusterPort:      "5555",
					ClusterNodeID:    "sc1",
					ClusterIPAddress: "127.0.0.1",
				},
			},
			want: &Scheduler{
				config: config.Config{
					ListenerDriver:   "test",
					PublisherDriver:  "test",
					StoragePath:      getProjectPath() + "/tests/tempStorageNs1",
					ClusterPort:      "5555",
					ClusterNodeID:    "sc1",
					ClusterIPAddress: "127.0.0.1",
				},
				listener:     nil,
				publisher:    nil,
				processor:    nil,
				prioritizer:  nil,
				dataStorage:  storage.NewPqStorage(),
				inboundPool:  goconcurrentqueue.NewFIFO(),
				outboundPool: goconcurrentqueue.NewFIFO(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = os.RemoveAll(tt.args.config.StoragePath)
			if got := NewScheduler(context.Background(), tt.args.config); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want.config, got.config)
				assert.Equal(t, tt.want.dataStorage, got.dataStorage)
				assert.Equal(t, reflect.TypeOf(tt.want.inboundPool), reflect.TypeOf(got.inboundPool))
				assert.Equal(t, reflect.TypeOf(tt.want.outboundPool), reflect.TypeOf(got.outboundPool))
			}
		})
	}
}

func getProjectPath() string {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "..")
	return dir
}

func TestScheduler_BootListener(t *testing.T) {
	type fields struct {
		config       config.Config
		listener     listener.Listener
		publisher    publisher.Publisher
		processor    *processor.Processor
		prioritizer  *prioritizer.Prioritizer
		dataStorage  *storage.PqStorage
		inboundPool  *goconcurrentqueue.FIFO
		outboundPool *goconcurrentqueue.FIFO
	}
	dir := getProjectPath()
	tests := []struct {
		name      string
		fields    fields
		want      *listenerpubsub.Listener
		wantPanic bool
	}{
		{
			name: "Check listener boot with amqp driver",
			fields: fields{
				config: config.Config{
					LogFormat:                    "",
					LogLevel:                     "",
					ListenerDriver:               "amqp",
					PubsubListenerProjectID:      "",
					PubsubListenerSubscriptionID: "",
					PubsubListenerKeyFile:        "",
					PublisherDriver:              "",
					PubsubPublisherProjectID:     "",
					PubsubPublisherTopicID:       "",
					PubsubPublisherKeyFile:       "",
				},
				listener:     nil,
				publisher:    nil,
				processor:    nil,
				prioritizer:  nil,
				dataStorage:  nil,
				inboundPool:  nil,
				outboundPool: nil,
			},
			want:      nil,
			wantPanic: true,
		},
		{
			name: "Check listener boot with pubsub driver",
			fields: fields{
				config: config.Config{
					ListenerDriver:          "pubsub",
					PubsubListenerProjectID: "testProjectId",
					PubsubListenerKeyFile:   dir + "/tests/pubsub_cred_mock.json",
				},
				listener:     nil,
				publisher:    nil,
				processor:    nil,
				prioritizer:  nil,
				dataStorage:  nil,
				inboundPool:  nil,
				outboundPool: nil,
			},
			want:      &listenerpubsub.Listener{},
			wantPanic: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				config:       tt.fields.config,
				listener:     tt.fields.listener,
				publisher:    tt.fields.publisher,
				processor:    tt.fields.processor,
				prioritizer:  tt.fields.prioritizer,
				dataStorage:  tt.fields.dataStorage,
				inboundPool:  tt.fields.inboundPool,
				outboundPool: tt.fields.outboundPool,
			}
			if !tt.wantPanic {
				assert.NotPanics(t, func() {
					s.BootListener(context.Background())
				})
				if s.listener != nil {
					assert.IsType(t, tt.want, s.listener)
				} else {
					assert.Nil(t, tt.want, s.listener)
				}
			} else {
				assert.Panics(t, func() {
					s.BootListener(context.Background())
				})
			}
		})
	}
}

func TestScheduler_BootPrioritizer(t *testing.T) {
	type fields struct {
		config       config.Config
		listener     listener.Listener
		publisher    publisher.Publisher
		processor    *processor.Processor
		prioritizer  *prioritizer.Prioritizer
		dataStorage  *storage.PqStorage
		inboundPool  *goconcurrentqueue.FIFO
		outboundPool *goconcurrentqueue.FIFO
	}
	tests := []struct {
		name      string
		fields    fields
		wantPanic bool
	}{
		{
			name: "Check prioritizer boot with proper configuration",
			fields: fields{
				config:       config.Config{},
				listener:     nil,
				publisher:    nil,
				processor:    nil,
				prioritizer:  nil,
				dataStorage:  storage.NewPqStorage(),
				inboundPool:  goconcurrentqueue.NewFIFO(),
				outboundPool: nil,
			},
			wantPanic: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				config:       tt.fields.config,
				listener:     tt.fields.listener,
				publisher:    tt.fields.publisher,
				processor:    tt.fields.processor,
				prioritizer:  tt.fields.prioritizer,
				dataStorage:  tt.fields.dataStorage,
				inboundPool:  tt.fields.inboundPool,
				outboundPool: tt.fields.outboundPool,
			}
			if !tt.wantPanic {
				assert.NotPanics(t, func() {
					s.BootPrioritizer(context.Background())
				})
			} else {
				assert.Panics(t, func() {
					s.BootPrioritizer(context.Background())
				})
			}
		})
	}
}

func TestScheduler_BootProcessor(t *testing.T) {
	type fields struct {
		config       config.Config
		listener     listener.Listener
		publisher    publisher.Publisher
		processor    *processor.Processor
		prioritizer  *prioritizer.Prioritizer
		dataStorage  *storage.PqStorage
		inboundPool  *goconcurrentqueue.FIFO
		outboundPool *goconcurrentqueue.FIFO
	}
	tests := []struct {
		name      string
		fields    fields
		wantPanic bool
	}{
		{
			name: "Check processor boot with proper configuration",
			fields: fields{
				config:       config.Config{},
				listener:     nil,
				publisher:    nil,
				processor:    nil,
				prioritizer:  nil,
				dataStorage:  storage.NewPqStorage(),
				inboundPool:  nil,
				outboundPool: nil,
			},
			wantPanic: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				config:       tt.fields.config,
				listener:     tt.fields.listener,
				publisher:    tt.fields.publisher,
				processor:    tt.fields.processor,
				prioritizer:  tt.fields.prioritizer,
				dataStorage:  tt.fields.dataStorage,
				inboundPool:  tt.fields.inboundPool,
				outboundPool: tt.fields.outboundPool,
			}
			if !tt.wantPanic {
				assert.NotPanics(t, func() {
					s.BootProcessor(context.Background())
				})
			} else {
				assert.Panics(t, func() {
					s.BootProcessor(context.Background())
				})
			}
		})
	}
}

func TestScheduler_BootPublisher(t *testing.T) {
	type fields struct {
		config       config.Config
		listener     listener.Listener
		publisher    publisher.Publisher
		processor    *processor.Processor
		prioritizer  *prioritizer.Prioritizer
		dataStorage  *storage.PqStorage
		inboundPool  *goconcurrentqueue.FIFO
		outboundPool *goconcurrentqueue.FIFO
	}
	dir := getProjectPath()
	tests := []struct {
		name      string
		fields    fields
		want      *publisherpubsub.Publisher
		wantPanic bool
	}{
		{
			name: "Check publisher boot with improper configuration",
			fields: fields{
				config: config.Config{
					LogFormat:                    "",
					LogLevel:                     "",
					ListenerDriver:               "",
					PubsubListenerProjectID:      "",
					PubsubListenerSubscriptionID: "",
					PubsubListenerKeyFile:        "",
					PublisherDriver:              "amqp",
					PubsubPublisherProjectID:     "",
					PubsubPublisherTopicID:       "",
					PubsubPublisherKeyFile:       "",
				},
				listener:     nil,
				publisher:    nil,
				processor:    nil,
				prioritizer:  nil,
				dataStorage:  nil,
				inboundPool:  nil,
				outboundPool: nil,
			},
			want:      nil,
			wantPanic: true,
		},
		{
			name: "Check publisher boot with proper configuration",
			fields: fields{
				config: config.Config{
					PublisherDriver:          "pubsub",
					PubsubPublisherProjectID: "testProjectId",
					PubsubPublisherKeyFile:   dir + "/tests/pubsub_cred_mock.json",
				},
				listener:     nil,
				publisher:    nil,
				processor:    nil,
				prioritizer:  nil,
				dataStorage:  nil,
				inboundPool:  nil,
				outboundPool: nil,
			},
			want:      &publisherpubsub.Publisher{},
			wantPanic: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				config:       tt.fields.config,
				listener:     tt.fields.listener,
				publisher:    tt.fields.publisher,
				processor:    tt.fields.processor,
				prioritizer:  tt.fields.prioritizer,
				dataStorage:  tt.fields.dataStorage,
				inboundPool:  tt.fields.inboundPool,
				outboundPool: tt.fields.outboundPool,
			}
			if !tt.wantPanic {
				assert.NotPanics(t, func() {
					s.BootPublisher(context.Background())
				})
				if s.publisher != nil {
					assert.IsType(t, tt.want, s.publisher)
				} else {
					assert.Nil(t, tt.want, s.publisher)
				}
			} else {
				assert.Panics(t, func() {
					s.BootPublisher(context.Background())
				})
			}
		})
	}
}

func TestScheduler_GetConfig(t *testing.T) {
	type fields struct {
		config       config.Config
		listener     listener.Listener
		publisher    publisher.Publisher
		processor    *processor.Processor
		prioritizer  *prioritizer.Prioritizer
		dataStorage  *storage.PqStorage
		inboundPool  *goconcurrentqueue.FIFO
		outboundPool *goconcurrentqueue.FIFO
	}
	tests := []struct {
		name   string
		fields fields
		want   config.Config
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				config:       tt.fields.config,
				listener:     tt.fields.listener,
				publisher:    tt.fields.publisher,
				processor:    tt.fields.processor,
				prioritizer:  tt.fields.prioritizer,
				dataStorage:  tt.fields.dataStorage,
				inboundPool:  tt.fields.inboundPool,
				outboundPool: tt.fields.outboundPool,
			}
			if got := s.GetConfig(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestScheduler_GetDataStorage(t *testing.T) {
	type fields struct {
		config       config.Config
		listener     listener.Listener
		publisher    publisher.Publisher
		processor    *processor.Processor
		prioritizer  *prioritizer.Prioritizer
		dataStorage  *storage.PqStorage
		inboundPool  *goconcurrentqueue.FIFO
		outboundPool *goconcurrentqueue.FIFO
	}
	tests := []struct {
		name   string
		fields fields
		want   *storage.PqStorage
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				config:       tt.fields.config,
				listener:     tt.fields.listener,
				publisher:    tt.fields.publisher,
				processor:    tt.fields.processor,
				prioritizer:  tt.fields.prioritizer,
				dataStorage:  tt.fields.dataStorage,
				inboundPool:  tt.fields.inboundPool,
				outboundPool: tt.fields.outboundPool,
			}
			if got := s.GetDataStorage(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestScheduler_GetInboundPool(t *testing.T) {
	type fields struct {
		config       config.Config
		listener     listener.Listener
		publisher    publisher.Publisher
		processor    *processor.Processor
		prioritizer  *prioritizer.Prioritizer
		dataStorage  *storage.PqStorage
		inboundPool  *goconcurrentqueue.FIFO
		outboundPool *goconcurrentqueue.FIFO
	}
	tests := []struct {
		name   string
		fields fields
		want   *goconcurrentqueue.FIFO
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				config:       tt.fields.config,
				listener:     tt.fields.listener,
				publisher:    tt.fields.publisher,
				processor:    tt.fields.processor,
				prioritizer:  tt.fields.prioritizer,
				dataStorage:  tt.fields.dataStorage,
				inboundPool:  tt.fields.inboundPool,
				outboundPool: tt.fields.outboundPool,
			}
			if got := s.GetInboundPool(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestScheduler_GetOutboundPool(t *testing.T) {
	type fields struct {
		config       config.Config
		listener     listener.Listener
		publisher    publisher.Publisher
		processor    *processor.Processor
		prioritizer  *prioritizer.Prioritizer
		dataStorage  *storage.PqStorage
		inboundPool  *goconcurrentqueue.FIFO
		outboundPool *goconcurrentqueue.FIFO
	}
	tests := []struct {
		name   string
		fields fields
		want   *goconcurrentqueue.FIFO
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				config:       tt.fields.config,
				listener:     tt.fields.listener,
				publisher:    tt.fields.publisher,
				processor:    tt.fields.processor,
				prioritizer:  tt.fields.prioritizer,
				dataStorage:  tt.fields.dataStorage,
				inboundPool:  tt.fields.inboundPool,
				outboundPool: tt.fields.outboundPool,
			}
			if got := s.GetOutboundPool(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestScheduler_GetPublisher(t *testing.T) {
	type fields struct {
		publisher publisher.Publisher
	}
	tests := []struct {
		name   string
		fields fields
		want   publisher.Publisher
	}{
		{
			name: "Check publisher getter",
			fields: fields{
				publisher: new(publishertest.Publisher),
			},
			want: new(publishertest.Publisher),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				publisher: tt.fields.publisher,
			}
			if got := s.GetPublisher(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, *got)
			}
		})
	}
}

func TestScheduler_GetCluster(t *testing.T) {
	type fields struct {
		raftCluster *raft.Raft
	}
	tests := []struct {
		name   string
		fields fields
		want   *raft.Raft
	}{
		{
			name: "Check cluster getter",
			fields: fields{
				raftCluster: new(raft.Raft),
			},
			want: new(raft.Raft),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				raftCluster: tt.fields.raftCluster,
			}
			if got := s.GetCluster(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestScheduler_GetListener(t *testing.T) {
	type fields struct {
		listener listener.Listener
	}
	tests := []struct {
		name   string
		fields fields
		want   listener.Listener
	}{
		{
			name: "Check listener getter",
			fields: fields{
				listener: new(listenertest.Listener),
			},
			want: new(listenertest.Listener),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				listener: tt.fields.listener,
			}
			if got := s.GetListener(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, *got)
			}
		})
	}
}

func mockPubsubListenerClient(ctx context.Context, t *testing.T, pubsubServerConn *grpc.ClientConn, cfg config.Config) (*pubsub.Client, *pubsub.Topic) {
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

func mockPubsubPublisherClient(ctx context.Context, t *testing.T, pubsubServerConn *grpc.ClientConn, cfg config.Config) (*pubsub.Client, *pubsub.Topic) {
	pubsubClient, _ := pubsub.NewClient(ctx, "", option.WithGRPCConn(pubsubServerConn))
	topic, err := pubsubClient.CreateTopic(ctx, cfg.PubsubPublisherTopicID)
	if err != nil {
		t.Error(err)
	}
	return pubsubClient, topic
}

func TestScheduler_Run(t *testing.T) {
	type fields struct {
		config       config.Config
		dataStorage  *storage.PqStorage
		inboundPool  *goconcurrentqueue.FIFO
		outboundPool *goconcurrentqueue.FIFO
	}

	dir := getProjectPath()

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
		publish []pubsub.Message
		want    []pubsub.Message
		time    processor.CurrentTimeChecker
	}{
		{
			name: "Test pubsub to pubsub event scheduler with all available messages",
			fields: fields{
				config: config.Config{
					ListenerDriver:           "pubsub",
					PubsubListenerProjectID:  "testProjectId",
					PubsubListenerKeyFile:    dir + "/tests/pubsub_cred_mock.json",
					PublisherDriver:          "pubsub",
					PubsubPublisherProjectID: "testProjectId",
					PubsubPublisherKeyFile:   dir + "/tests/pubsub_cred_mock.json",
					StoragePath:              getProjectPath() + "/tests/tempStorageSt1",
					ClusterPort:              "5558",
					ClusterNodeID:            "st1",
					ClusterIPAddress:         "127.0.0.1",
				},
				inboundPool:  goconcurrentqueue.NewFIFO(),
				outboundPool: goconcurrentqueue.NewFIFO(),
				dataStorage:  storage.NewPqStorage(),
			},
			publish: []pubsub.Message{{
				Data:       []byte("msg1"),
				Attributes: map[string]string{"available_at": "1000"},
			}, {
				Data:       []byte("msg2"),
				Attributes: map[string]string{"available_at": "1100"},
			}, {
				Data:       []byte("msg3"),
				Attributes: map[string]string{"available_at": "1200"},
			}},
			time: processor.RealTime{},
			want: []pubsub.Message{{
				Data: []byte("msg1"),
			}, {
				Data: []byte("msg2"),
			}, {
				Data: []byte("msg3"),
			}},
			wantErr: false,
		},
		{
			name: "Test pubsub to pubsub event scheduler with some unavailable messages",
			fields: fields{
				config: config.Config{
					ListenerDriver:           "pubsub",
					PubsubListenerProjectID:  "testProjectId",
					PubsubListenerKeyFile:    dir + "/tests/pubsub_cred_mock.json",
					PublisherDriver:          "pubsub",
					PubsubPublisherProjectID: "testProjectId",
					PubsubPublisherKeyFile:   dir + "/tests/pubsub_cred_mock.json",
					StoragePath:              getProjectPath() + "/tests/tempStorageSt2",
					ClusterPort:              "5559",
					ClusterNodeID:            "st2",
					ClusterIPAddress:         "127.0.0.1",
				},
				inboundPool:  goconcurrentqueue.NewFIFO(),
				outboundPool: goconcurrentqueue.NewFIFO(),
				dataStorage:  storage.NewPqStorage(),
			},
			publish: []pubsub.Message{{
				Data:       []byte("msg1"),
				Attributes: map[string]string{"available_at": "1000"},
			}, {
				Data:       []byte("msg2"),
				Attributes: map[string]string{"available_at": "1100"},
			}, {
				Data:       []byte("msg3"),
				Attributes: map[string]string{"available_at": "1200"},
			}},
			time: processor.NewMockTime(time.Unix(1150, 0)),
			want: []pubsub.Message{{
				Data: []byte("msg1"),
			}, {
				Data: []byte("msg2"),
			}},
			wantErr: false,
		},
	}

	for testID, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			s := &Scheduler{
				config:       tt.fields.config,
				inboundPool:  tt.fields.inboundPool,
				outboundPool: tt.fields.outboundPool,
				dataStorage:  tt.fields.dataStorage,
			}

			// mock pubsub servers
			sourcePubsubServer := pstest.NewServer()
			defer func() {
				_ = sourcePubsubServer.Close()
			}()

			destPubsubServer := pstest.NewServer()
			defer func() {
				_ = destPubsubServer.Close()
			}()

			// mock individual context for each test
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*6)
			defer cancel()

			_ = os.RemoveAll(tt.fields.config.StoragePath)
			s.BootCluster(ctx)
			defer func() {
				_ = s.raftCluster.Shutdown()
			}()

			// bootstrap single node test cluster
			s.raftCluster.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(s.config.ClusterNodeID),
					Address:  raft.ServerAddress(s.config.ClusterIPAddress + ":" + s.config.ClusterPort),
				},
			}})

			tt.fields.config.PubsubListenerSubscriptionID = "mocklistener" + strconv.Itoa(testID)
			tt.fields.config.PubsubPublisherTopicID = "mockpublisher" + strconv.Itoa(testID)

			// make pubsub listener client-server connection
			sourcePubsubServerConn, _ := grpc.Dial(sourcePubsubServer.Addr, grpc.WithInsecure())
			defer func() {
				_ = sourcePubsubServerConn.Close()
			}()
			sourcePubsubClient, topic := mockPubsubListenerClient(ctx, t, sourcePubsubServerConn, tt.fields.config)
			pubsubListener := &listenerpubsub.Listener{}
			_ = pubsubListener.Boot(ctx, tt.fields.config, s.inboundPool, s.raftCluster)
			pubsubListener.SetPubsubClient(sourcePubsubClient)

			// make pubsub publisher client-server connection
			destPubsubServerConn, _ := grpc.Dial(destPubsubServer.Addr, grpc.WithInsecure())
			defer func() {
				_ = destPubsubServerConn.Close()
			}()
			destPubsubClient, _ := mockPubsubPublisherClient(ctx, t, destPubsubServerConn, tt.fields.config)
			pubsubPublisher := &publisherpubsub.Publisher{}
			_ = pubsubPublisher.Boot(ctx, tt.fields.config, s.outboundPool)
			pubsubPublisher.SetPubsubClient(destPubsubClient)

			// publish test messages
			for _, msg := range tt.publish {
				sourcePubsubServer.Publish(topic.String(), msg.Data, msg.Attributes)
			}

			s.publisher = pubsubPublisher
			s.listener = pubsubListener
			s.BootPrioritizer(ctx)
			s.BootProcessor(ctx)
			s.processor.SetTime(tt.time)

			// execute application
			err := s.Run(ctx)

			assert.Nil(t, err)

			// read pushed messages
			got := []pubsub.Message{}

			for _, msg := range destPubsubServer.Messages() {
				got = append(got, pubsub.Message{
					Data:       msg.Data,
					Attributes: msg.Attributes,
				})
			}

			// compare received messages
			if !reflect.DeepEqual(got, tt.want) {
				assert.ElementsMatch(t, tt.want, got)
			}

			if !tt.wantErr {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}

		})
	}
}
