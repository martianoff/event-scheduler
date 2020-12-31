package scheduler

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/listener"
	"github.com/maksimru/event-scheduler/prioritizer"
	"github.com/maksimru/event-scheduler/processor"
	"github.com/maksimru/event-scheduler/publisher"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"path"
	"reflect"
	"runtime"
	"testing"
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
				config: config.Config{},
			},
			want: &Scheduler{
				config:       config.Config{},
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
			if got := NewScheduler(tt.args.config); !reflect.DeepEqual(got, tt.want) {
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
	dir := path.Join(path.Dir(filename), "../..")
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
		want   *publisher.Publisher
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
			if got := s.GetPublisher(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
