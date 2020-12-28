package listener_pubsub

import (
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestListenerPubsub_Boot(t *testing.T) {
	type fields struct {
		config      config.Config
		inboundPool *goconcurrentqueue.FIFO
	}
	type args struct {
		config      config.Config
		inboundPool *goconcurrentqueue.FIFO
	}
	cfg := config.Config{}
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
				config:      cfg,
				inboundPool: inboundPool,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &ListenerPubsub{
				config:      tt.fields.config,
				inboundPool: tt.fields.inboundPool,
			}
			if !tt.wantErr {
				assert.NoError(t, l.Boot(tt.args.config, tt.args.inboundPool))
			} else {
				assert.Error(t, l.Boot(tt.args.config, tt.args.inboundPool))
			}
		})
	}
}
