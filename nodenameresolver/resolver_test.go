package nodenameresolver

import (
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestResolve(t *testing.T) {
	type args struct {
		address string
	}
	tests := []struct {
		name string
		args args
		want raft.ServerID
	}{
		{
			name: "Check node id generator",
			args: args{
				"127.0.0.1:1000",
			},
			want: "243ef5f445279e4f51423fdcde9cf733",
		},
		{
			name: "Check node id generator with another port",
			args: args{
				"127.0.0.1:1001",
			},
			want: "7be05fe69870977b0e4b2fe9bc76646b",
		},
		{
			name: "Check node id generator with another host",
			args: args{
				"localhost:1000",
			},
			want: "23d3a4b92a2ec9fd91d5baced328da1b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Resolve(tt.args.address); got != tt.want {
				assert.Equal(t, tt.args.address, tt.want)
			}
		})
	}
}

func Test_getHash(t *testing.T) {
	type args struct {
		text string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Check hash function",
			args: args{
				text: "test",
			},
			want: "098f6bcd4621d373cade4e832627b4f6",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getHash(tt.args.text); got != tt.want {
				assert.Equal(t, tt.args.text, tt.want)
			}
		})
	}
}
