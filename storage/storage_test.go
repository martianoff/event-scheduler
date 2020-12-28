package storage

import (
	"github.com/maksimru/go-hpds/priorityqueue"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync"
	"testing"
)

func TestNewPqStorage(t *testing.T) {
	tests := []struct {
		name string
		want *PqStorage
	}{
		{
			name: "PqStorage creation",
			want: &PqStorage{
				mutex: &sync.Mutex{},
				dataStorage: priorityqueue.NewPriorityQueue(
					priorityqueue.NewStringPrioritizedValueList(make([]priorityqueue.StringPrioritizedValue, 0)),
					NewStringMinPriorityComparator(),
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewPqStorage(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestNewStringMinPriorityComparator(t *testing.T) {
	tests := []struct {
		name string
		want *StringMinPriorityComparator
	}{
		{
			name: "String min priority comparator creation",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				NewStringMinPriorityComparator()
			})
		})
	}
}

func TestPqStorage_CheckScheduled(t *testing.T) {
	type fields struct {
		mutex       *sync.Mutex
		dataStorage *priorityqueue.PriorityQueue
	}
	type args struct {
		nowTimestamp int
	}
	storage := NewPqStorage()
	storage.dataStorage.Enqueue(priorityqueue.NewStringPrioritizedValue("msg1", 1000))
	storage.dataStorage.Enqueue(priorityqueue.NewStringPrioritizedValue("msg2", 1001))
	storage.dataStorage.Enqueue(priorityqueue.NewStringPrioritizedValue("msg3", 1002))
	emptyStorage := NewPqStorage()
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "Check scheduled messages are ready",
			fields: fields{
				storage.mutex,
				storage.dataStorage,
			},
			args: args{
				1005,
			},
			want: true,
		},
		{
			name: "Check scheduled messages are partially ready",
			fields: fields{
				storage.mutex,
				storage.dataStorage,
			},
			args: args{
				1000,
			},
			want: true,
		},
		{
			name: "Check scheduled messages are not ready",
			fields: fields{
				storage.mutex,
				storage.dataStorage,
			},
			args: args{
				999,
			},
			want: false,
		},
		{
			name: "Check scheduled messages are not ready in empty storage",
			fields: fields{
				emptyStorage.mutex,
				emptyStorage.dataStorage,
			},
			args: args{
				999,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:       tt.fields.mutex,
				dataStorage: tt.fields.dataStorage,
			}
			if got := p.CheckScheduled(tt.args.nowTimestamp); got != tt.want {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestPqStorage_Dequeue(t *testing.T) {
	type fields struct {
		mutex       *sync.Mutex
		dataStorage *priorityqueue.PriorityQueue
	}
	storage := NewPqStorage()
	storage.dataStorage.Enqueue(priorityqueue.NewStringPrioritizedValue("msg1", 1000))
	storage.dataStorage.Enqueue(priorityqueue.NewStringPrioritizedValue("msg2", 1100))
	tests := []struct {
		name   string
		fields fields
		want   priorityqueue.StringPrioritizedValue
	}{
		{
			name: "Check storage extraction",
			fields: fields{
				mutex:       storage.mutex,
				dataStorage: storage.dataStorage,
			},
			want: priorityqueue.NewStringPrioritizedValue("msg1", 1000),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:       tt.fields.mutex,
				dataStorage: tt.fields.dataStorage,
			}
			if got := p.Dequeue(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestPqStorage_Enqueue(t *testing.T) {
	type fields struct {
		mutex       *sync.Mutex
		dataStorage *priorityqueue.PriorityQueue
	}
	type args struct {
		value priorityqueue.PrioritizedValue
	}
	storage := NewPqStorage()
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "Check storage insertion",
			fields: fields{
				mutex:       storage.mutex,
				dataStorage: storage.dataStorage,
			},
			args: args{
				priorityqueue.NewStringPrioritizedValue("msg1", 1000),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:       tt.fields.mutex,
				dataStorage: tt.fields.dataStorage,
			}
			assert.NotPanics(t, func() {
				p.Enqueue(tt.args.value)
			})
		})
	}
}

func TestStringMinPriorityComparator_Equal(t *testing.T) {
	type args struct {
		value1 priorityqueue.StringPrioritizedValue
		value2 priorityqueue.StringPrioritizedValue
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Check min timestamp prioritizer equals comparator",
			args: args{
				value1: priorityqueue.NewStringPrioritizedValue("msg1", 100),
				value2: priorityqueue.NewStringPrioritizedValue("msg2", 100),
			},
			want: true,
		},
		{
			name: "Check min timestamp prioritizer not equals comparator",
			args: args{
				value1: priorityqueue.NewStringPrioritizedValue("msg1", 100),
				value2: priorityqueue.NewStringPrioritizedValue("msg2", 101),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmp := NewStringMinPriorityComparator()
			if got := cmp.Equal(tt.args.value1, tt.args.value2); got != tt.want {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestStringMinPriorityComparator_Less(t *testing.T) {
	type args struct {
		value1 priorityqueue.StringPrioritizedValue
		value2 priorityqueue.StringPrioritizedValue
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Check that event with lower timestamp get higher priority",
			args: args{
				value1: priorityqueue.NewStringPrioritizedValue("msg1", 100),
				value2: priorityqueue.NewStringPrioritizedValue("msg2", 102),
			},
			want: false,
		},
		{
			name: "Check that event with higher timestamp get lower priority",
			args: args{
				value1: priorityqueue.NewStringPrioritizedValue("msg1", 200),
				value2: priorityqueue.NewStringPrioritizedValue("msg2", 101),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmp := NewStringMinPriorityComparator()
			if got := cmp.Less(tt.args.value1, tt.args.value2); got != tt.want {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
