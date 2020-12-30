package processor

import (
	"context"
	"github.com/maksimru/event-scheduler/publisher"
	publisherpubsub "github.com/maksimru/event-scheduler/publisher/pubsub"
	"github.com/maksimru/event-scheduler/storage"
	"github.com/stretchr/testify/assert"
	"testing"
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
	publisherProvider := new(publisherpubsub.PubsubPublisher)
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
