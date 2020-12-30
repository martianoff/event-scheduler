package pubsubpublisher

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/message"
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
	type fields struct {
		config       config.Config
		outboundPool *goconcurrentqueue.FIFO
	}
	type args struct {
		context      context.Context
		config       config.Config
		outboundPool *goconcurrentqueue.FIFO
	}
	cfg := config.Config{}
	outboundPool := goconcurrentqueue.NewFIFO()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Checks pubsub publisher boot",
			fields: fields{
				config:       cfg,
				outboundPool: outboundPool,
			},
			args: args{
				context:      context.Background(),
				config:       cfg,
				outboundPool: outboundPool,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PubsubPublisher{
				config:       tt.fields.config,
				outboundPool: tt.fields.outboundPool,
			}
			if !tt.wantErr {
				assert.NoError(t, p.Boot(tt.args.context, tt.args.config, tt.args.outboundPool))
			} else {
				assert.Error(t, p.Boot(tt.args.context, tt.args.config, tt.args.outboundPool))
			}
		})
	}
}

func TestPubsubPublisher_Push(t *testing.T) {
	type fields struct {
		config       config.Config
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
				config:       config.Config{},
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
				config:       config.Config{},
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
			p := &PubsubPublisher{
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
					LogFormat:                    "",
					LogLevel:                     "",
					ListenerDriver:               "",
					PubsubListenerProjectID:      "",
					PubsubListenerSubscriptionID: "",
					PubsubListenerKeyFile:        "",
					PublisherDriver:              "",
					PubsubPublisherProjectID:     "testProjectId",
					PubsubPublisherTopicID:       "",
					PubsubPublisherKeyFile:       dir + "/tests/pubsub_cred_mock.json",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := makePubsubClient(tt.args.ctx, tt.args.config)
			defer client.Close()
			if (err != nil) != tt.wantErr {
				t.Errorf("makePubsubClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func mockPubsubClient(ctx context.Context, t *testing.T, pubsubServerConn *grpc.ClientConn, cfg config.Config) (*pubsub.Client, *pubsub.Topic) {
	pubsubClient, _ := pubsub.NewClient(ctx, "", option.WithGRPCConn(pubsubServerConn))
	topic, err := pubsubClient.CreateTopic(ctx, cfg.PubsubPublisherTopicID)
	if err != nil {
		t.Error(err)
	}
	pubsubClient.CreateSubscription(ctx, cfg.PubsubPublisherTopicID, pubsub.SubscriptionConfig{
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
			defer pubsubServer.Close()

			// mock individual context for each test
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*3)
			defer cancel()

			cfg := config.Config{
				PubsubPublisherTopicID: "mocksubscription" + strconv.Itoa(testID),
			}
			// make pubsub client-server connection
			pubsubServerConn, _ := grpc.Dial(pubsubServer.Addr, grpc.WithInsecure())
			defer pubsubServerConn.Close()
			pubsubClient, _ := mockPubsubClient(ctx, t, pubsubServerConn, cfg)

			p := &PubsubPublisher{
				config:       cfg,
				outboundPool: tt.fields.outboundPool,
				client:       pubsubClient,
				context:      ctx,
			}

			// mock outbound queue
			for _, msg := range tt.publish {
				p.outboundPool.Enqueue(msg)
			}

			if !tt.wantErr {
				assert.NoError(t, p.Dispatch())
			} else {
				assert.Error(t, p.Dispatch())
			}

			// read pushed messages
			got := []pubsub.Message{}

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
