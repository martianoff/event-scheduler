package pubsub

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"context"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/message"
	pubsubconfig "github.com/maksimru/event-scheduler/publisher/pubsub/config"
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

func TestNewPubSubPublisher(t *testing.T) {
	type args struct {
		context context.Context
		config  channel.DestinationConfig
	}
	dir := getProjectPath()
	tests := []struct {
		name string
		args args
		want Publisher
	}{
		{
			name: "Checks pubsub publisher constructor",
			args: args{
				context: context.Background(),
				config: pubsubconfig.DestinationConfig{
					ProjectID: "pr",
					TopicID:   "topic",
					KeyFile:   dir + "/tests/pubsub_cred_mock.json",
				},
			},
			want: Publisher{
				config: pubsubconfig.DestinationConfig{
					ProjectID: "pr",
					TopicID:   "topic",
					KeyFile:   dir + "/tests/pubsub_cred_mock.json",
				},
				context: context.Background(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPubSubPublisher(tt.args.context, tt.args.config)
			tt.want.SetPubsubClient(p.client)
			assert.Equal(t, tt.want, *p)
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
	tests := []struct {
		name    string
		wantErr bool
		publish []message.Message
		want    []pubsub.Message
	}{
		{
			name:    "Check pubsub publisher can publish single message",
			publish: []message.Message{message.NewMessage("foo", 1000)},
			wantErr: false,
			want: []pubsub.Message{{
				Data: []byte("foo"),
			}},
		},
		{
			name: "Check pubsub publisher can publish multiple message",
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
			ctx, cancel := context.WithTimeout(ctx, time.Second*2)
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
				config:  cfg,
				client:  pubsubClient,
				context: ctx,
			}

			// mock outbound queue
			for _, msg := range tt.publish {
				if !tt.wantErr {
					assert.NoError(t, p.Dispatch(msg))
				} else {
					assert.Error(t, p.Dispatch(msg))
				}
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

func TestPublisher_Close(t *testing.T) {
	type fields struct {
		config  pubsubconfig.DestinationConfig
		context context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Check publisher close",
			fields: fields{
				config:  pubsubconfig.DestinationConfig{},
				context: context.Background(),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := getProjectPath()
			client, _ := makePubsubClient(tt.fields.context, pubsubconfig.DestinationConfig{
				ProjectID: "pr",
				TopicID:   "topic",
				KeyFile:   dir + "/tests/pubsub_cred_mock.json",
			})
			p := Publisher{
				config:  tt.fields.config,
				client:  client,
				context: tt.fields.context,
			}
			err := p.Close()
			if !tt.wantErr {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
