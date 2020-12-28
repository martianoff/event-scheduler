package publisher_pubsub

import (
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/message"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPubsubPublisher_Boot(t *testing.T) {
	type fields struct {
		config       config.Config
		outboundPool *goconcurrentqueue.FIFO
	}
	type args struct {
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
				assert.NoError(t, p.Boot(tt.args.config, tt.args.outboundPool))
			} else {
				assert.Error(t, p.Boot(tt.args.config, tt.args.outboundPool))
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
