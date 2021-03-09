package dispatcher

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/publisher"
	publisherpubsub "github.com/maksimru/event-scheduler/publisher/pubsub"
	pubsubpublisherconfig "github.com/maksimru/event-scheduler/publisher/pubsub/config"
	publishertest "github.com/maksimru/event-scheduler/publisher/test"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"path"
	"runtime"
	"testing"
	"time"
)

func TestMessageDispatcher_Dispatch(t *testing.T) {
	type fields struct {
		outboundPool *goconcurrentqueue.FIFO
		dataStorage  *storage.PqStorage
	}
	tests := []struct {
		name              string
		fields            fields
		publish           []message.Message
		want              []message.Message
		wantErr           bool
		channelID         string
		availableChannels []channel.Channel
	}{
		{
			name: "Check pubsub publisher can publish single message",
			fields: fields{
				outboundPool: goconcurrentqueue.NewFIFO(),
				dataStorage:  storage.NewPqStorage(),
			},
			publish:   []message.Message{message.NewMessage("foo", 1000)},
			wantErr:   false,
			want:      []message.Message{message.NewMessage("foo", 1000)},
			channelID: "ch1",
			availableChannels: []channel.Channel{
				{
					ID: "ch1",
					Destination: channel.Destination{
						Driver: "test",
					},
				},
			},
		},
		{
			name: "Check pubsub publisher can publish multiple message",
			fields: fields{
				outboundPool: goconcurrentqueue.NewFIFO(),
				dataStorage:  storage.NewPqStorage(),
			},
			publish: []message.Message{
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg2", 1100),
				message.NewMessage("msg3", 1200),
			},
			wantErr: false,
			want: []message.Message{
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg2", 1100),
				message.NewMessage("msg3", 1200),
			},
			channelID: "ch1",
			availableChannels: []channel.Channel{
				{
					ID: "ch1",
					Destination: channel.Destination{
						Driver: "test",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*2)
			defer cancel()

			d := &MessageDispatcher{
				outboundPool: tt.fields.outboundPool,
				context:      ctx,
				dataStorage:  tt.fields.dataStorage,
			}

			mockPublisher := publishertest.NewTestPublisher()
			d.publishers = make(map[string]publisher.Publisher)
			d.publishers[tt.channelID] = mockPublisher

			for _, c := range tt.availableChannels {
				_, _ = tt.fields.dataStorage.AddChannel(c)
			}

			// mock outbound queue
			for _, msg := range tt.publish {
				_ = d.Push(msg, tt.channelID)
			}

			if !tt.wantErr {
				assert.NoError(t, d.Dispatch())
			} else {
				assert.Error(t, d.Dispatch())
			}

			// compare received messages
			assert.ElementsMatch(t, tt.want, mockPublisher.GetDispatched())

		})
	}
}

func TestMessageDispatcher_Push(t *testing.T) {
	type fields struct {
		outboundPool *goconcurrentqueue.FIFO
		context      context.Context
		publishers   map[string]publisher.Publisher
		dataStorage  *storage.PqStorage
	}
	type args struct {
		msg       message.Message
		channelID string
	}
	lockedDispatchQueue := goconcurrentqueue.NewFIFO()
	lockedDispatchQueue.Lock()
	tests := []struct {
		name              string
		fields            fields
		args              args
		availableChannels []channel.Channel
		wantErr           bool
	}{
		{
			name: "Checks test publisher pending push method",
			fields: fields{
				context:      context.Background(),
				outboundPool: goconcurrentqueue.NewFIFO(),
				dataStorage:  storage.NewPqStorage(),
			},
			args: args{
				message.NewMessage("foo", 1000),
				"ch1",
			},
			availableChannels: []channel.Channel{
				{
					ID: "ch1",
					Destination: channel.Destination{
						Driver: "test",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Checks test publisher pending push with locked dispatch pool",
			fields: fields{
				context:      context.Background(),
				outboundPool: lockedDispatchQueue,
				dataStorage:  storage.NewPqStorage(),
			},
			args: args{
				message.NewMessage("foo", 1000),
				"ch1",
			},
			availableChannels: []channel.Channel{
				{
					ID: "ch1",
					Destination: channel.Destination{
						Driver: "test",
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &MessageDispatcher{
				outboundPool: tt.fields.outboundPool,
				context:      tt.fields.context,
				publishers:   tt.fields.publishers,
				dataStorage:  tt.fields.dataStorage,
			}
			for _, c := range tt.availableChannels {
				_, _ = tt.fields.dataStorage.AddChannel(c)
			}
			if !tt.wantErr {
				assert.NoError(t, d.Push(tt.args.msg, tt.args.channelID))
			} else {
				assert.Error(t, d.Push(tt.args.msg, tt.args.channelID))
			}
		})
	}
}

func getProjectPath() string {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "..")
	return dir
}

func TestMessageDispatcher_getChannelPublisher(t *testing.T) {
	type fields struct {
		outboundPool *goconcurrentqueue.FIFO
		context      context.Context
		publishers   map[string]publisher.Publisher
		dataStorage  *storage.PqStorage
	}
	type args struct {
		channelID string
	}
	dir := getProjectPath()
	tests := []struct {
		name              string
		fields            fields
		args              args
		availableChannels []channel.Channel
		want              publisher.Publisher
		wantErr           bool
	}{
		{
			name: "Check test publisher creation",
			fields: fields{
				outboundPool: goconcurrentqueue.NewFIFO(),
				context:      context.Background(),
				dataStorage:  storage.NewPqStorage(),
			},
			args: args{
				"ch1",
			},
			availableChannels: []channel.Channel{
				{
					ID: "ch1",
					Destination: channel.Destination{
						Driver: "test",
					},
				},
			},
			want:    &publishertest.Publisher{},
			wantErr: false,
		},
		{
			name: "Check unknown publisher creation",
			fields: fields{
				outboundPool: goconcurrentqueue.NewFIFO(),
				context:      context.Background(),
				dataStorage:  storage.NewPqStorage(),
			},
			args: args{
				"ch1",
			},
			availableChannels: []channel.Channel{},
			want:              &publishertest.Publisher{},
			wantErr:           true,
		},
		{
			name: "Check test pubsub publisher creation",
			fields: fields{
				outboundPool: goconcurrentqueue.NewFIFO(),
				context:      context.Background(),
				dataStorage:  storage.NewPqStorage(),
			},
			args: args{
				"ch1",
			},
			availableChannels: []channel.Channel{
				{
					ID: "ch1",
					Destination: channel.Destination{
						Driver: "pubsub",
						Config: pubsubpublisherconfig.DestinationConfig{
							ProjectID: "pr",
							TopicID:   "topic",
							KeyFile:   dir + "/tests/pubsub_cred_mock.json",
						},
					},
				},
			},
			want:    &publisherpubsub.Publisher{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDispatcher(tt.fields.context, tt.fields.outboundPool, tt.fields.dataStorage)
			for _, c := range tt.availableChannels {
				_, _ = tt.fields.dataStorage.AddChannel(c)
			}
			got, err := d.getChannelPublisher(tt.args.channelID)
			if !tt.wantErr {
				assert.NoError(t, err)
				assert.IsType(t, tt.want, got)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestMessageForDelivery_GetMessage(t *testing.T) {
	type fields struct {
		msg       message.Message
		channelID string
	}
	tests := []struct {
		name   string
		fields fields
		want   message.Message
	}{
		{
			name: "Check get message",
			fields: fields{
				msg: message.Message{
					AvailableAt: 10,
					Body:        "foo",
				},
				channelID: "ch1",
			},
			want: message.Message{
				AvailableAt: 10,
				Body:        "foo",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := MessageForDelivery{
				msg:       tt.fields.msg,
				channelID: tt.fields.channelID,
			}
			assert.Equal(t, tt.want, m.GetMessage())
		})
	}
}

func TestNewDispatcher(t *testing.T) {
	type args struct {
		ctx          context.Context
		outboundPool *goconcurrentqueue.FIFO
		dataStorage  *storage.PqStorage
	}
	q := goconcurrentqueue.NewFIFO()
	s := storage.NewPqStorage()
	tests := []struct {
		name string
		args args
		want *MessageDispatcher
	}{
		{
			name: "Check constructor",
			args: args{
				ctx:          context.Background(),
				outboundPool: q,
				dataStorage:  s,
			},
			want: &MessageDispatcher{
				outboundPool: q,
				context:      context.Background(),
				dataStorage:  s,
				publishers:   make(map[string]publisher.Publisher),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewDispatcher(tt.args.ctx, tt.args.outboundPool, tt.args.dataStorage)
			assert.Equal(t, tt.want, got)
		})
	}
}
