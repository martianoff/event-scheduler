package test

import (
	"github.com/maksimru/event-scheduler/message"
	"testing"
)

func TestPublisher_Push(t *testing.T) {
	type args struct {
		in0 message.Message
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "Test push function on test publisher driver",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Publisher{}
			if err := p.Push(tt.args.in0); (err != nil) != tt.wantErr {
				t.Errorf("Push() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
