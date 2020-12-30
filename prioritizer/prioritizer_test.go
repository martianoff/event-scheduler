package prioritizer

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"testing"
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
