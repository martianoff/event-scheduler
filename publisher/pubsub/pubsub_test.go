package pubsub

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/message"
	pubsubconfig "github.com/maksimru/event-scheduler/publisher/config"
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

func TestPubsubPublisher_Boot(t *testing.T) {
	type args struct {
		context      context.Context
		config       channel.DestinationConfig
		outboundPool *goconcurrentqueue.FIFO
		channel      channel.Channel
	}
	dir := getProjectPath()
	outboundPool := goconcurrentqueue.NewFIFO()
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Checks pubsub publisher boot",
			args: args{
				context: context.Background(),
				channel: channel.Channel{
					Destination: channel.Destination{
						Driver: "pubsub",
						Config: pubsubconfig.DestinationConfig{
							ProjectID: "pr",
							TopicID:   "topic",
							KeyFile:   dir + "/tests/pubsub_cred_mock.json",
						},
					},
				},
				outboundPool: outboundPool,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Publisher{}
			if !tt.wantErr {
				assert.NoError(t, p.Boot(tt.args.context, tt.args.channel, tt.args.outboundPool))
			} else {
				assert.Error(t, p.Boot(tt.args.context, tt.args.channel, tt.args.outboundPool))
			}
		})
	}
}

func TestPubsubPublisher_Push(t *testing.T) {
	type fields struct {
		config       pubsubconfig.DestinationConfig
		outboundPool *goconcurrentqueue.FIFO
	}
	type args struct {
		msg message.Message
	}
	lockedDispatchQueue := goconcurrentqueue.NewFIFO()
	lockedDispatchQueue.Lock()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Checks pubsub publisher pending push method",
			fields: fields{
				config:       pubsubconfig.DestinationConfig{},
				outboundPool: goconcurrentqueue.NewFIFO(),
			},
			args: args{
				message.NewMessage("foo", 1000),
			},
			wantErr: false,
		},
		{
			name: "Checks pubsub publisher pending push with locked dispatch pool",
			fields: fields{
				config:       pubsubconfig.DestinationConfig{},
				outboundPool: lockedDispatchQueue,
			},
			args: args{
				message.NewMessage("foo", 1000),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Publisher{
				config:       tt.fields.config,
				outboundPool: tt.fields.outboundPool,
			}
			if !tt.wantErr {
				assert.NoError(t, p.Push(tt.args.msg))
			} else {
				assert.Error(t, p.Push(tt.args.msg))
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
		config pubsubconfig.DestinationConfig
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
				config: pubsubconfig.DestinationConfig{
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

func mockPubsubClient(ctx context.Context, t *testing.T, pubsubServerConn *grpc.ClientConn, cfg pubsubconfig.DestinationConfig) (*pubsub.Client, *pubsub.Topic) {
	pubsubClient, _ := pubsub.NewClient(ctx, "", option.WithGRPCConn(pubsubServerConn))
	topic, err := pubsubClient.CreateTopic(ctx, cfg.TopicID)
	if err != nil {
		t.Error(err)
	}
	_, _ = pubsubClient.CreateSubscription(ctx, cfg.TopicID, pubsub.SubscriptionConfig{
		Topic: topic,
	})
	return pubsubClient, topic
}

func TestPubsubPublisher_Dispatch(t *testing.T) {
	type fields struct {
		outboundPool *goconcurrentqueue.FIFO
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
		publish []message.Message
		want    []pubsub.Message
	}{
		{
			name: "Check pubsub publisher can publish single message",
			fields: fields{
				outboundPool: goconcurrentqueue.NewFIFO(),
			},
			publish: []message.Message{message.NewMessage("foo", 1000)},
			wantErr: false,
			want: []pubsub.Message{{
				Data: []byte("foo"),
			}},
		},
		{
			name: "Check pubsub publisher can publish multiple message",
			fields: fields{
				outboundPool: goconcurrentqueue.NewFIFO(),
			},
			publish: []message.Message{
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg2", 1100),
				message.NewMessage("msg3", 1200),
			},
			wantErr: false,
			want: []pubsub.Message{{
				Data: []byte("msg1"),
			}, {
				Data: []byte("msg2"),
			}, {
				Data: []byte("msg3"),
			}},
		},
	}
	for testID, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pubsubServer := pstest.NewServer()
			defer func() {
				_ = pubsubServer.Close()
			}()
			// mock individual context for each test
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*3)
			defer cancel()

			cfg := pubsubconfig.DestinationConfig{
				TopicID: "mocksubscription" + strconv.Itoa(testID),
			}
			// make pubsub client-server connection
			pubsubServerConn, _ := grpc.Dial(pubsubServer.Addr, grpc.WithInsecure())
			defer func() {
				_ = pubsubServerConn.Close()
			}()
			pubsubClient, _ := mockPubsubClient(ctx, t, pubsubServerConn, cfg)

			p := &Publisher{
				config:       cfg,
				outboundPool: tt.fields.outboundPool,
				client:       pubsubClient,
				context:      ctx,
			}

			// mock outbound queue
			for _, msg := range tt.publish {
				_ = p.outboundPool.Enqueue(msg)
			}

			if !tt.wantErr {
				assert.NoError(t, p.Dispatch())
			} else {
				assert.Error(t, p.Dispatch())
			}

			// read pushed messages
			var got []pubsub.Message

			for _, msg := range pubsubServer.Messages() {
				got = append(got, pubsub.Message{
					Data:       msg.Data,
					Attributes: msg.Attributes,
				})
			}

			// compare received messages
			if !reflect.DeepEqual(got, tt.want) {
				assert.ElementsMatch(t, tt.want, got)
			}
		})
	}
}
