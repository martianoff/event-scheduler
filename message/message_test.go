package message

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestMessage_GetAvailableAt(t *testing.T) {
	type fields struct {
		availableAt int
		body        interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{
			name: "Zero AvailableAt",
			fields: fields{
				availableAt: 0,
				body:        "foo",
			},
			want: 0,
		},
		{
			name: "Nonzero AvailableAt",
			fields: fields{
				availableAt: 10000,
				body:        "foo",
			},
			want: 10000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := Message{
				AvailableAt: tt.fields.availableAt,
				Body:        tt.fields.body,
			}
			if got := msg.GetAvailableAt(); got != tt.want {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestMessage_GetBody(t *testing.T) {
	type fields struct {
		availableAt int
		body        interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   interface{}
	}{
		{
			name: "Empty Body",
			fields: fields{
				availableAt: 1000,
				body:        "",
			},
			want: "",
		},
		{
			name: "Nonempty Body",
			fields: fields{
				availableAt: 10000,
				body:        "foo",
			},
			want: "foo",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := Message{
				AvailableAt: tt.fields.availableAt,
				Body:        tt.fields.body,
			}
			if got := msg.GetBody(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestNewMessage(t *testing.T) {
	type args struct {
		body        interface{}
		availableAt int
	}
	tests := []struct {
		name string
		args args
		want Message
	}{
		{
			name: "Message with string Body",
			args: args{
				availableAt: 1000,
				body:        "foo",
			},
			want: Message{
				AvailableAt: 1000,
				Body:        "foo",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewMessage(tt.args.body, tt.args.availableAt); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
