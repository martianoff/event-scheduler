package processor

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/publisher"
	"github.com/maksimru/event-scheduler/publisher/pubsub"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/maksimru/go-hpds/priorityqueue"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func TestProcessor_Boot(t *testing.T) {
	type fields struct {
		publisher   publisher.Publisher
		dataStorage *storage.PqStorage
	}
	type args struct {
		publisher   publisher.Publisher
		dataStorage *storage.PqStorage
		context     context.Context
	}
	publisherProvider := new(pubsub.Publisher)
	dataStorage := storage.NewPqStorage()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Check processor boot",
			fields: fields{
				publisher:   publisherProvider,
				dataStorage: dataStorage,
			},
			args: args{
				publisher:   publisherProvider,
				dataStorage: dataStorage,
				context:     context.Background(),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Processor{
				publisher:   tt.fields.publisher,
				dataStorage: tt.fields.dataStorage,
			}
			if !tt.wantErr {
				assert.NoError(t, p.Boot(tt.args.context, tt.args.publisher, tt.args.dataStorage))
			} else {
				assert.Error(t, p.Boot(tt.args.context, tt.args.publisher, tt.args.dataStorage))
			}
		})
	}
}

func TestProcessor_Process(t *testing.T) {
	type fields struct {
		dataStorage *storage.PqStorage
		time        CurrentTimeChecker
	}

	tests := []struct {
		name          string
		fields        fields
		wantErr       bool
		now           time.Time
		storageData   []priorityqueue.StringPrioritizedValue
		wantStorage   []priorityqueue.StringPrioritizedValue
		wantPublished []message.Message
	}{
		{
			name: "Check prioritizer can move old messages to dispatch",
			fields: fields{
				dataStorage: storage.NewPqStorage(),
				time:        RealTime{},
			},
			storageData: []priorityqueue.StringPrioritizedValue{
				priorityqueue.NewStringPrioritizedValue("msg2", 400),
				priorityqueue.NewStringPrioritizedValue("msg3", 600),
				priorityqueue.NewStringPrioritizedValue("msg1", 1000),
				priorityqueue.NewStringPrioritizedValue("msg5", 1200),
				priorityqueue.NewStringPrioritizedValue("msg4", 2000),
			},
			wantStorage: []priorityqueue.StringPrioritizedValue{},
			wantPublished: []message.Message{
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg5", 1200),
				message.NewMessage("msg4", 2000),
			},
			wantErr: false,
		},
		{
			name: "Check prioritizer don't move messages if they are not ready for dispatch",
			fields: fields{
				dataStorage: storage.NewPqStorage(),
				time:        NewMockTime(time.Unix(0, 0)), // simulate 0 timestamp
			},
			storageData: []priorityqueue.StringPrioritizedValue{
				priorityqueue.NewStringPrioritizedValue("msg2", 400),
				priorityqueue.NewStringPrioritizedValue("msg3", 600),
				priorityqueue.NewStringPrioritizedValue("msg1", 1000),
				priorityqueue.NewStringPrioritizedValue("msg5", 1200),
				priorityqueue.NewStringPrioritizedValue("msg4", 2000),
			},
			wantStorage: []priorityqueue.StringPrioritizedValue{
				priorityqueue.NewStringPrioritizedValue("msg2", 400),
				priorityqueue.NewStringPrioritizedValue("msg3", 600),
				priorityqueue.NewStringPrioritizedValue("msg1", 1000),
				priorityqueue.NewStringPrioritizedValue("msg5", 1200),
				priorityqueue.NewStringPrioritizedValue("msg4", 2000),
			},
			wantPublished: []message.Message{},
			wantErr:       false,
		},
		{
			name: "Check prioritizer can partially dispatch prepared messages on time",
			fields: fields{
				dataStorage: storage.NewPqStorage(),
				time:        NewMockTime(time.Unix(600, 0)), // simulate 600 timestamp
			},
			storageData: []priorityqueue.StringPrioritizedValue{
				priorityqueue.NewStringPrioritizedValue("msg2", 400),
				priorityqueue.NewStringPrioritizedValue("msg3", 600),
				priorityqueue.NewStringPrioritizedValue("msg1", 1000),
				priorityqueue.NewStringPrioritizedValue("msg5", 1200),
				priorityqueue.NewStringPrioritizedValue("msg4", 2000),
			},
			wantStorage: []priorityqueue.StringPrioritizedValue{
				priorityqueue.NewStringPrioritizedValue("msg1", 1000),
				priorityqueue.NewStringPrioritizedValue("msg5", 1200),
				priorityqueue.NewStringPrioritizedValue("msg4", 2000),
			},
			wantPublished: []message.Message{
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*2)
			defer cancel()

			outboundQueue := goconcurrentqueue.NewFIFO()
			publisherInstance := new(pubsub.Publisher)
			publisherInstance.Boot(ctx, config.Config{}, outboundQueue)

			p := &Processor{
				publisher:   publisherInstance,
				dataStorage: tt.fields.dataStorage,
				context:     ctx,
				time:        tt.fields.time,
			}

			// insert requested input
			for _, msg := range tt.storageData {
				p.dataStorage.Enqueue(msg)
			}

			// execute prioritizer
			if !tt.wantErr {
				assert.NoError(t, p.Process())
			} else {
				assert.Error(t, p.Process())
			}
			// validate unprocessed records
			gotStorage := []priorityqueue.StringPrioritizedValue{}
			for !p.dataStorage.IsEmpty() {
				gotStorage = append(gotStorage, p.dataStorage.Dequeue())
			}
			if !reflect.DeepEqual(gotStorage, tt.wantStorage) {
				assert.Equal(t, tt.wantStorage, gotStorage)
			}
			// validate results
			gotPublished := []message.Message{}
			for outboundQueue.GetLen() > 0 {
				msg, _ := outboundQueue.Dequeue()
				gotPublished = append(gotPublished, msg.(message.Message))
			}
			if !reflect.DeepEqual(gotPublished, tt.wantPublished) {
				assert.Equal(t, tt.wantPublished, gotPublished)
			}
		})
	}
}
