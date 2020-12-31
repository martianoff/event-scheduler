package pubsublistener

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/go-hpds/priorityqueue"
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
		inboundPool *goconcurrentqueue.FIFO
	}
	type args struct {
		context     context.Context
		config      config.Config
		inboundPool *goconcurrentqueue.FIFO
	}
	dir := getProjectPath()
	cfg := config.Config{
		PubsubListenerProjectID: "testProjectId",
		PubsubListenerKeyFile:   dir + "/tests/pubsub_cred_mock.json",
	}
	inboundPool := goconcurrentqueue.NewFIFO()
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
				inboundPool: inboundPool,
			},
			args: args{
				context:     context.Background(),
				config:      cfg,
				inboundPool: inboundPool,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &PubsubListener{
				config:      tt.fields.config,
				inboundPool: tt.fields.inboundPool,
			}
			if !tt.wantErr {
				assert.NoError(t, l.Boot(tt.args.context, tt.args.config, tt.args.inboundPool))
			} else {
				assert.Error(t, l.Boot(tt.args.context, tt.args.config, tt.args.inboundPool))
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
	topic, err := pubsubClient.CreateTopic(ctx, cfg.PubsubListenerSubscriptionID)
	if err != nil {
		t.Error(err)
	}
	pubsubClient.CreateSubscription(ctx, cfg.PubsubListenerSubscriptionID, pubsub.SubscriptionConfig{
		Topic: topic,
	})
	return pubsubClient, topic
}

func TestListenerPubsub_Listen(t *testing.T) {
	type fields struct {
		inboundPool *goconcurrentqueue.FIFO
	}
	pubsubServer := pstest.NewServer()
	defer pubsubServer.Close()

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
		publish []*pubsub.Message
		want    []priorityqueue.StringPrioritizedValue
	}{
		{
			name: "Check pubsub listener can receive single message with available_at attribute",
			fields: fields{
				inboundPool: goconcurrentqueue.NewFIFO(),
			},
			publish: []*pubsub.Message{{
				Data:       []byte("foo"),
				Attributes: map[string]string{"available_at": "1000"},
			}},
			wantErr: false,
			want:    []priorityqueue.StringPrioritizedValue{priorityqueue.NewStringPrioritizedValue("foo", 1000)},
		},
		{
			name: "Check pubsub listener can receive single message without available_at attribute",
			fields: fields{
				inboundPool: goconcurrentqueue.NewFIFO(),
			},
			publish: []*pubsub.Message{{
				Data:       []byte("foo"),
				Attributes: map[string]string{},
			}},
			wantErr: false,
			want:    []priorityqueue.StringPrioritizedValue{},
		},
		{
			name: "Check pubsub listener can receive single message with wrong available_at attribute",
			fields: fields{
				inboundPool: goconcurrentqueue.NewFIFO(),
			},
			publish: []*pubsub.Message{{
				Data:       []byte("foo"),
				Attributes: map[string]string{"available_at": "foo"},
			}},
			wantErr: false,
			want:    []priorityqueue.StringPrioritizedValue{},
		},
		{
			name: "Check pubsub listener can receive multiple messages",
			fields: fields{
				inboundPool: goconcurrentqueue.NewFIFO(),
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
			wantErr: false,
			want: []priorityqueue.StringPrioritizedValue{
				priorityqueue.NewStringPrioritizedValue("msg1", 1000),
				priorityqueue.NewStringPrioritizedValue("msg2", 1100),
				priorityqueue.NewStringPrioritizedValue("msg3", 1200),
			},
		},
	}
	for testID, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// mock individual context for each test
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*1)
			defer cancel()

			cfg := config.Config{
				PubsubListenerSubscriptionID: "mocksubscription" + strconv.Itoa(testID),
			}

			// make pubsub client-server connection
			pubsubServerConn, _ := grpc.Dial(pubsubServer.Addr, grpc.WithInsecure())
			defer pubsubServerConn.Close()
			pubsubClient, topic := mockPubsubClient(ctx, t, pubsubServerConn, cfg)

			l := &PubsubListener{
				config:      cfg,
				inboundPool: tt.fields.inboundPool,
				client:      pubsubClient,
				context:     ctx,
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
			got := []priorityqueue.StringPrioritizedValue{}
			for l.inboundPool.GetLen() > 0 {
				item, _ := l.inboundPool.Dequeue()
				got = append(got, item.(priorityqueue.StringPrioritizedValue))
			}
			// compare received messages
			if !reflect.DeepEqual(got, tt.want) {
				assert.ElementsMatch(t, tt.want, got)
			}
		})
	}
}
