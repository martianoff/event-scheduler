package prioritizer

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/maksimru/go-hpds/priorityqueue"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func TestPrioritizer_Boot(t *testing.T) {
	type fields struct {
		inboundPool *goconcurrentqueue.FIFO
		dataStorage *storage.PqStorage
	}
	type args struct {
		inboundPool *goconcurrentqueue.FIFO
		dataStorage *storage.PqStorage
		context     context.Context
	}
	inboundPool := goconcurrentqueue.NewFIFO()
	dataStorage := storage.NewPqStorage()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Check prioritizer boot",
			fields: fields{
				inboundPool: inboundPool,
				dataStorage: dataStorage,
			},
			args: args{
				inboundPool: inboundPool,
				dataStorage: dataStorage,
				context:     context.Background(),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Prioritizer{
				inboundPool: tt.fields.inboundPool,
				dataStorage: tt.fields.dataStorage,
			}
			if !tt.wantErr {
				assert.NoError(t, p.Boot(tt.args.context, tt.args.inboundPool, tt.args.dataStorage))
			} else {
				assert.Error(t, p.Boot(tt.args.context, tt.args.inboundPool, tt.args.dataStorage))
			}
		})
	}
}

func TestPrioritizer_Process(t *testing.T) {
	type fields struct {
		inboundPool *goconcurrentqueue.FIFO
		dataStorage *storage.PqStorage
	}
	tests := []struct {
		name        string
		fields      fields
		wantErr     bool
		inboundMsgs []priorityqueue.StringPrioritizedValue
		want        []priorityqueue.StringPrioritizedValue
	}{
		{
			name: "Check prioritizer can transfer one message to the storage",
			fields: fields{
				inboundPool: goconcurrentqueue.NewFIFO(),
				dataStorage: storage.NewPqStorage(),
			},
			inboundMsgs: []priorityqueue.StringPrioritizedValue{
				priorityqueue.NewStringPrioritizedValue("msg1", 1000),
			},
			want: []priorityqueue.StringPrioritizedValue{
				priorityqueue.NewStringPrioritizedValue("msg1", 1000),
			},
			wantErr: false,
		},
		{
			name: "Check prioritizer can transfer more than single message with right priority",
			fields: fields{
				inboundPool: goconcurrentqueue.NewFIFO(),
				dataStorage: storage.NewPqStorage(),
			},
			inboundMsgs: []priorityqueue.StringPrioritizedValue{
				priorityqueue.NewStringPrioritizedValue("msg1", 1000),
				priorityqueue.NewStringPrioritizedValue("msg2", 400),
				priorityqueue.NewStringPrioritizedValue("msg3", 600),
				priorityqueue.NewStringPrioritizedValue("msg4", 2000),
				priorityqueue.NewStringPrioritizedValue("msg5", 1200),
			},
			want: []priorityqueue.StringPrioritizedValue{
				priorityqueue.NewStringPrioritizedValue("msg2", 400),
				priorityqueue.NewStringPrioritizedValue("msg3", 600),
				priorityqueue.NewStringPrioritizedValue("msg1", 1000),
				priorityqueue.NewStringPrioritizedValue("msg5", 1200),
				priorityqueue.NewStringPrioritizedValue("msg4", 2000),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()

			p := &Prioritizer{
				inboundPool: tt.fields.inboundPool,
				dataStorage: tt.fields.dataStorage,
				context:     ctx,
			}

			// insert requested input
			for _, msg := range tt.inboundMsgs {
				_ = p.inboundPool.Enqueue(msg)
			}

			// execute prioritizer
			if !tt.wantErr {
				assert.NoError(t, p.Process())
			} else {
				assert.Error(t, p.Process())
			}

			assert.Equal(t, 0, p.inboundPool.GetLen())

			// validate results
			var got []priorityqueue.StringPrioritizedValue
			for !p.dataStorage.IsEmpty() {
				got = append(got, p.dataStorage.Dequeue())
			}

			if !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
