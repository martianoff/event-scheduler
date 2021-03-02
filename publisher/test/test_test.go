package test

import (
	"github.com/maksimru/event-scheduler/message"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPublisher_Push(t *testing.T) {
	type args struct {
		msg message.Message
	}
	tests := []struct {
		name    string
		args    args
		want    []message.Message
		wantErr bool
	}{
		{
			name: "Test push function on test publisher driver",
			args: args{
				msg: message.Message{
					AvailableAt: 0,
					Body:        "test",
				},
			},
			want: []message.Message{
				{
					AvailableAt: 0,
					Body:        "test",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Publisher{}
			err := p.Dispatch(tt.args.msg)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, p.GetDispatched())
			}
		})
	}
}
