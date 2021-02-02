package prioritizer

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/event-scheduler/storage"
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
		inboundMsgs []message.Message
		want        []message.Message
	}{
		{
			name: "Check prioritizer can transfer one message to the storage",
			fields: fields{
				inboundPool: goconcurrentqueue.NewFIFO(),
				dataStorage: storage.NewPqStorage(),
			},
			inboundMsgs: []message.Message{
				message.NewMessage("msg1", 1000),
			},
			want: []message.Message{
				message.NewMessage("msg1", 1000),
			},
			wantErr: false,
		},
		{
			name: "Check prioritizer can transfer more than single message with right priority",
			fields: fields{
				inboundPool: goconcurrentqueue.NewFIFO(),
				dataStorage: storage.NewPqStorage(),
			},
			inboundMsgs: []message.Message{
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
				message.NewMessage("msg4", 2000),
				message.NewMessage("msg5", 1200),
			},
			want: []message.Message{
				message.NewMessage("msg2", 400),
				message.NewMessage("msg3", 600),
				message.NewMessage("msg1", 1000),
				message.NewMessage("msg5", 1200),
				message.NewMessage("msg4", 2000),
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
			var got []message.Message
			for !p.dataStorage.IsEmpty() {
				got = append(got, p.dataStorage.Dequeue())
			}

			if !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
