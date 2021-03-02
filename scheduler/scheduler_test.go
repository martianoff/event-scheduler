package scheduler

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/hashicorp/raft"
	"github.com/labstack/echo/v4"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/dispatcher"
	"github.com/maksimru/event-scheduler/listener"
	listenerpubsub "github.com/maksimru/event-scheduler/listener/pubsub"
	pubsublistenerconfig "github.com/maksimru/event-scheduler/listener/pubsub/config"
	"github.com/maksimru/event-scheduler/prioritizer"
	"github.com/maksimru/event-scheduler/processor"
	publisherpubsub "github.com/maksimru/event-scheduler/publisher/pubsub"
	pubsubpublisherconfig "github.com/maksimru/event-scheduler/publisher/pubsub/config"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"net/http"
	"os"
	"path"
	"reflect"
	"runtime"
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
					ListenerDriver:  "test",
					PublisherDriver: "test",
					StoragePath:     getProjectPath() + "/tests/tempStorageNs1",
					ClusterNodePort: "5554",
					ClusterNodeHost: "localhost",
				},
			},
			want: &Scheduler{
				config: config.Config{
					ListenerDriver:  "test",
					PublisherDriver: "test",
					StoragePath:     getProjectPath() + "/tests/tempStorageNs1",
					ClusterNodePort: "5554",
					ClusterNodeHost: "localhost",
				},
				listeners:        make(map[string]listener.Listener),
				listenerRunning:  make(map[string]bool),
				processors:       make(map[string]*processor.Processor),
				processorRunning: make(map[string]bool),
				dataStorage:      storage.NewPqStorage(),
				outboundPool:     goconcurrentqueue.NewFIFO(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = os.RemoveAll(tt.args.config.StoragePath)
			if got := NewScheduler(context.Background(), tt.args.config); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want.config, got.config)
				assert.Equal(t, tt.want.dataStorage, got.dataStorage)
				assert.Equal(t, tt.want.listenerRunning, got.listenerRunning)
				assert.Equal(t, tt.want.listeners, got.listeners)
				assert.Equal(t, tt.want.processorRunning, got.processorRunning)
				assert.Equal(t, tt.want.processors, got.processors)
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
		listener listener.Listener
		channel  channel.Channel
	}
	dir := getProjectPath()
	tests := []struct {
		name      string
		fields    fields
		want      *listenerpubsub.Listener
		wantError bool
		wantPanic bool
	}{
		{
			name: "Check listener boot with amqp driver",
			fields: fields{
				channel: channel.Channel{ID: "ch1", Source: channel.Source{
					Driver: "amqp",
				}},
			},
			want:      nil,
			wantPanic: false,
			wantError: true,
		},
		{
			name: "Check listener boot with pubsub driver",
			fields: fields{
				channel: channel.Channel{ID: "ch1", Source: channel.Source{
					Driver: "pubsub",
					Config: pubsublistenerconfig.SourceConfig{
						ProjectID:      "testProjectId",
						SubscriptionID: "testSubscriptionId",
						KeyFile:        dir + "/tests/pubsub_cred_mock.json",
					},
				}},
			},
			want:      &listenerpubsub.Listener{},
			wantPanic: false,
			wantError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				listeners:       make(map[string]listener.Listener),
				listenerRunning: make(map[string]bool),
			}
			if !tt.wantPanic {
				var e error
				assert.NotPanics(t, func() {
					e = s.BootListener(context.Background(), tt.fields.channel)
				})
				if tt.wantError {
					assert.Error(t, e)
				} else {
					if _, has := s.listeners[tt.fields.channel.ID]; has {
						assert.IsType(t, tt.want, s.listeners[tt.fields.channel.ID])
					} else {
						assert.Nil(t, tt.want, s.listeners[tt.fields.channel.ID])
					}
				}
			} else {
				assert.Panics(t, func() {
					s.BootListener(context.Background(), tt.fields.channel)
				})
			}
		})
	}
}

func TestScheduler_BootPrioritizer(t *testing.T) {
	type fields struct {
		config       config.Config
		listener     listener.Listener
		dispatcher   dispatcher.Dispatcher
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
				dispatcher:   nil,
				processor:    nil,
				prioritizer:  nil,
				dataStorage:  storage.NewPqStorage(),
				outboundPool: nil,
			},
			wantPanic: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				config:       tt.fields.config,
				dispatcher:   tt.fields.dispatcher,
				prioritizer:  tt.fields.prioritizer,
				dataStorage:  tt.fields.dataStorage,
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
		channel      channel.Channel
		context      context.Context
		outboundPool *goconcurrentqueue.FIFO
		dispatcher   dispatcher.Dispatcher
		processor    *processor.Processor
		dataStorage  *storage.PqStorage
		cluster      *raft.Raft
	}
	dir := getProjectPath()
	tests := []struct {
		name      string
		fields    fields
		wantPanic bool
	}{
		{
			name: "Check processor boot with improper configuration",
			fields: fields{
				context:      context.Background(),
				outboundPool: goconcurrentqueue.NewFIFO(),
				channel:      channel.Channel{ID: "ch1"},
			},
			wantPanic: false,
		},
		{
			name: "Check processor boot with proper configuration",
			fields: fields{
				context:      context.Background(),
				outboundPool: goconcurrentqueue.NewFIFO(),
				channel: channel.Channel{ID: "ch1", Destination: channel.Destination{
					Driver: "pubsub",
					Config: pubsubpublisherconfig.DestinationConfig{
						ProjectID: "testProjectId",
						TopicID:   "testTopicId",
						KeyFile:   dir + "/tests/pubsub_cred_mock.json",
					},
				}},
			},
			wantPanic: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				dispatcher:       dispatcher.NewDispatcher(tt.fields.context, tt.fields.outboundPool, tt.fields.dataStorage),
				dataStorage:      tt.fields.dataStorage,
				outboundPool:     tt.fields.outboundPool,
				raftCluster:      tt.fields.cluster,
				processors:       make(map[string]*processor.Processor),
				processorRunning: make(map[string]bool),
			}
			if !tt.wantPanic {
				assert.NotPanics(t, func() {
					s.BootProcessor(context.Background(), tt.fields.channel)
				})
			} else {
				assert.Panics(t, func() {
					s.BootProcessor(context.Background(), tt.fields.channel)
				})
			}
		})
	}
}

func TestScheduler_GetConfig(t *testing.T) {
	type fields struct {
		config       config.Config
		listener     listener.Listener
		dispatcher   dispatcher.Dispatcher
		processor    *processor.Processor
		prioritizer  *prioritizer.Prioritizer
		dataStorage  *storage.PqStorage
		outboundPool *goconcurrentqueue.FIFO
	}
	tests := []struct {
		name   string
		fields fields
		want   config.Config
	}{
		{
			name: "Check config getter",
			fields: fields{
				config: config.Config{
					LogLevel: "error",
				},
			},
			want: config.Config{
				LogLevel: "error",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				config: tt.fields.config,
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
		dispatcher   dispatcher.Dispatcher
		processor    *processor.Processor
		prioritizer  *prioritizer.Prioritizer
		dataStorage  *storage.PqStorage
		outboundPool *goconcurrentqueue.FIFO
	}
	s := storage.NewPqStorage()
	tests := []struct {
		name   string
		fields fields
		want   *storage.PqStorage
	}{
		{
			name: "Check datastorage getter",
			fields: fields{
				dataStorage: s,
			},
			want: s,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				dataStorage: tt.fields.dataStorage,
			}
			if got := s.GetDataStorage(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestScheduler_GetOutboundPool(t *testing.T) {
	type fields struct {
		config       config.Config
		listener     listener.Listener
		dispatcher   dispatcher.Dispatcher
		processor    *processor.Processor
		prioritizer  *prioritizer.Prioritizer
		dataStorage  *storage.PqStorage
		outboundPool *goconcurrentqueue.FIFO
	}
	o := goconcurrentqueue.NewFIFO()
	tests := []struct {
		name   string
		fields fields
		want   *goconcurrentqueue.FIFO
	}{
		{
			name: "Check outbound pool getter",
			fields: fields{
				outboundPool: o,
			},
			want: o,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				outboundPool: tt.fields.outboundPool,
			}
			if got := s.GetOutboundPool(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
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

func mockPubsubListenerClient(ctx context.Context, t *testing.T, pubsubServerConn *grpc.ClientConn, projectID string, subscriptionID string) (*pubsub.Client, *pubsub.Topic) {
	pubsubClient, _ := pubsub.NewClient(ctx, projectID, option.WithGRPCConn(pubsubServerConn))
	topic, err := pubsubClient.CreateTopic(ctx, subscriptionID)
	if err != nil {
		t.Error(err)
	}
	_, _ = pubsubClient.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
		Topic: topic,
	})
	return pubsubClient, topic
}

func mockPubsubPublisherClient(ctx context.Context, t *testing.T, pubsubServerConn *grpc.ClientConn, projectID string, topicID string) (*pubsub.Client, *pubsub.Topic) {
	pubsubClient, _ := pubsub.NewClient(ctx, projectID, option.WithGRPCConn(pubsubServerConn))
	topic, err := pubsubClient.CreateTopic(ctx, topicID)
	if err != nil {
		t.Error(err)
	}
	return pubsubClient, topic
}

func TestScheduler_Run(t *testing.T) {
	type fields struct {
		config       config.Config
		dataStorage  *storage.PqStorage
		outboundPool *goconcurrentqueue.FIFO
		channel      channel.Channel
		httpServer   *echo.Echo
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
				channel: channel.Channel{
					ID: "ch1",
					Source: channel.Source{
						Driver: "pubsub",
						Config: pubsublistenerconfig.SourceConfig{
							KeyFile:        dir + "/tests/pubsub_cred_mock.json",
							SubscriptionID: "mocklistener_1",
						},
					},
					Destination: channel.Destination{
						Driver: "pubsub",
						Config: pubsubpublisherconfig.DestinationConfig{
							KeyFile: dir + "/tests/pubsub_cred_mock.json",
							TopicID: "mockpublisher_1",
						},
					},
				},
				config: config.Config{
					StoragePath:         getProjectPath() + "/tests/tempStorageSt1",
					ClusterNodePort:     "5558",
					ClusterNodeHost:     "localhost",
					ClusterInitialNodes: "localhost:5558",
					APIPort:             "5561",
				},
				outboundPool: goconcurrentqueue.NewFIFO(),
				dataStorage:  storage.NewPqStorage(),
				httpServer:   echo.New(),
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
				channel: channel.Channel{
					ID: "ch1",
					Source: channel.Source{
						Driver: "pubsub",
						Config: pubsublistenerconfig.SourceConfig{
							KeyFile:        dir + "/tests/pubsub_cred_mock.json",
							SubscriptionID: "mocklistener_1",
						},
					},
					Destination: channel.Destination{
						Driver: "pubsub",
						Config: pubsubpublisherconfig.DestinationConfig{
							KeyFile: dir + "/tests/pubsub_cred_mock.json",
							TopicID: "mockpublisher_1",
						},
					},
				},
				config: config.Config{
					StoragePath:         getProjectPath() + "/tests/tempStorageSt2",
					ClusterNodePort:     "5559",
					ClusterNodeHost:     "localhost",
					ClusterInitialNodes: "localhost:5559",
					APIPort:             "5562",
				},
				outboundPool: goconcurrentqueue.NewFIFO(),
				dataStorage:  storage.NewPqStorage(),
				httpServer:   echo.New(),
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// mock individual context for each test
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*10)
			defer cancel()

			s := &Scheduler{
				config:           tt.fields.config,
				outboundPool:     tt.fields.outboundPool,
				dataStorage:      tt.fields.dataStorage,
				dispatcher:       dispatcher.NewDispatcher(ctx, tt.fields.outboundPool, tt.fields.dataStorage),
				httpServer:       tt.fields.httpServer,
				listenerRunning:  make(map[string]bool),
				listeners:        make(map[string]listener.Listener),
				processorRunning: make(map[string]bool),
				processors:       make(map[string]*processor.Processor),
			}

			// bind webserver with context for graceful termination
			/*
				if err := s.httpServer.Shutdown(ctx); err != nil && err != http.ErrServerClosed {
					s.httpServer.Logger.Fatal(err)
				}
			*/
			defer func() {
				_ = s.httpServer.Close()
			}()

			// mock pubsub servers
			sourcePubsubServer := pstest.NewServer()
			defer func() {
				_ = sourcePubsubServer.Close()
			}()

			destPubsubServer := pstest.NewServer()
			defer func() {
				_ = destPubsubServer.Close()
			}()

			_ = os.RemoveAll(tt.fields.config.StoragePath)
			s.BootCluster(ctx)
			defer func() {
				_ = s.raftCluster.Shutdown()
			}()

			s.BootPrioritizer(ctx)

			c := tt.fields.channel

			scfg := c.Source.Config.(pubsublistenerconfig.SourceConfig)

			dcfg := c.Destination.Config.(pubsubpublisherconfig.DestinationConfig)

			// make pubsub listener client-server connection
			sourcePubsubServerConn, _ := grpc.Dial(sourcePubsubServer.Addr, grpc.WithInsecure())
			defer func() {
				_ = sourcePubsubServerConn.Close()
			}()
			sourcePubsubClient, topic := mockPubsubListenerClient(ctx, t, sourcePubsubServerConn, scfg.ProjectID, scfg.SubscriptionID)
			pubsubListener := &listenerpubsub.Listener{}
			_ = pubsubListener.Boot(ctx, c, s.prioritizer)
			pubsubListener.SetPubsubClient(sourcePubsubClient)

			// make pubsub dispatcher client-server connection
			destPubsubServerConn, _ := grpc.Dial(destPubsubServer.Addr, grpc.WithInsecure())
			defer func() {
				_ = destPubsubServerConn.Close()
			}()
			destPubsubClient, _ := mockPubsubPublisherClient(ctx, t, destPubsubServerConn, dcfg.ProjectID, dcfg.TopicID)
			pubsubPublisher := publisherpubsub.NewPubSubPublisher(ctx, dcfg)
			pubsubPublisher.SetPubsubClient(destPubsubClient)

			d := s.dispatcher.(*dispatcher.MessageDispatcher)
			d.SetPublisher(c.ID, pubsubPublisher)

			// publish test messages
			for _, msg := range tt.publish {
				sourcePubsubServer.Publish(topic.String(), msg.Data, msg.Attributes)
			}

			// wait for election
			time.Sleep(time.Second * 4)

			// register channel
			_, _ = s.dataStorage.AddChannel(c)

			// run mock listener
			s.listenerRunning[c.ID] = true
			s.listeners[c.ID] = pubsubListener
			go func(s *Scheduler) {
				_ = pubsubListener.Listen()
			}(s)

			// run mock processor
			s.processorRunning[c.ID] = true
			s.BootProcessor(ctx, c)
			go func(s *Scheduler) {
				s.processors[c.ID].SetTime(tt.time)
				_ = s.processors[c.ID].Process()
			}(s)

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

func TestScheduler_ClusterLeaderChangeCallback(t *testing.T) {
	type fields struct {
		config          config.Config
		listener        listener.Listener
		dispatcher      dispatcher.Dispatcher
		processor       *processor.Processor
		prioritizer     *prioritizer.Prioritizer
		dataStorage     *storage.PqStorage
		outboundPool    *goconcurrentqueue.FIFO
		raftCluster     *raft.Raft
		httpServer      *echo.Echo
		listenerRunning bool
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "Check listeners and processors stops on lose of leadership and starts on gain of leadership",
			fields: fields{
				config: config.Config{
					ListenerDriver:      "test",
					PublisherDriver:     "test",
					StoragePath:         getProjectPath() + "/tests/tempStorageLt1",
					ClusterNodePort:     "5553",
					ClusterNodeHost:     "localhost",
					ClusterInitialNodes: "localhost:5553",
					APIPort:             "5563",
				},
				outboundPool: goconcurrentqueue.NewFIFO(),
				dataStorage:  storage.NewPqStorage(),
				httpServer:   echo.New(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// mock individual context for each test
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*8)
			defer cancel()

			s := &Scheduler{
				config:           tt.fields.config,
				outboundPool:     tt.fields.outboundPool,
				dataStorage:      tt.fields.dataStorage,
				httpServer:       tt.fields.httpServer,
				listenerRunning:  make(map[string]bool),
				listeners:        make(map[string]listener.Listener),
				processorRunning: make(map[string]bool),
				processors:       make(map[string]*processor.Processor),
			}
			s.channelHandler = channel.NewEventHandler(s.channelUpdated(ctx), s.channelDeleted(ctx), s.channelAdded(ctx))

			// bind webserver with context for graceful termination
			if err := s.httpServer.Shutdown(ctx); err != nil && err != http.ErrServerClosed {
				s.httpServer.Logger.Fatal(err)
			}
			defer func() {
				_ = s.httpServer.Close()
			}()

			_ = os.RemoveAll(tt.fields.config.StoragePath)
			s.BootCluster(ctx)
			defer func() {
				_ = s.raftCluster.Shutdown()
			}()
			s.BootChannelManager(ctx)

			time.Sleep(time.Second * 2)
			assert.Equal(t, true, s.listenerRunning[defaultChannelName])
			assert.Equal(t, true, s.processorRunning[defaultChannelName])
			// emulate lose of leadership
			s.clusterLeaderChangeCallback(ctx, false)
			time.Sleep(time.Second * 2)
			assert.Equal(t, false, s.listenerRunning[defaultChannelName])
			assert.Equal(t, false, s.processorRunning[defaultChannelName])
			// emulate gain of leadership
			s.clusterLeaderChangeCallback(ctx, true)
			time.Sleep(time.Second * 2)
			assert.Equal(t, true, s.listenerRunning[defaultChannelName])
			assert.Equal(t, true, s.processorRunning[defaultChannelName])
		})
	}
}
