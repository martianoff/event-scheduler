package storage

import (
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/go-hpds/doublylinkedlist"
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
					NewPrioritizedNodePointerValueList(make([]PrioritizedNodePointer, 0)),
					NewPrioritizedNodePointerMinPriorityComparator(),
				),
				iterator: doublylinkedlist.NewDoublyLinkedList(),
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

func TestNewPrioritizedNodePointerMinPriorityComparator(t *testing.T) {
	tests := []struct {
		name string
		want *PrioritizedNodePointerMinPriorityComparator
	}{
		{
			name: "String min priority comparator creation",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				NewPrioritizedNodePointerMinPriorityComparator()
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
	storage.Enqueue(message.NewMessage("msg1", 1000))
	storage.Enqueue(message.NewMessage("msg2", 1001))
	storage.Enqueue(message.NewMessage("msg3", 1002))
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
	storage.Enqueue(message.NewMessage("msg1", 1000))
	storage.Enqueue(message.NewMessage("msg2", 1100))
	tests := []struct {
		name   string
		fields fields
		want   message.Message
	}{
		{
			name: "Check storage extraction",
			fields: fields{
				mutex:       storage.mutex,
				dataStorage: storage.dataStorage,
			},
			want: message.NewMessage("msg1", 1000),
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
		iterator    *doublylinkedlist.DoublyLinkedList
	}
	type args struct {
		value message.Message
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
				iterator:    doublylinkedlist.NewDoublyLinkedList(),
			},
			args: args{
				message.NewMessage("msg1", 1000),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:       tt.fields.mutex,
				dataStorage: tt.fields.dataStorage,
				iterator:    tt.fields.iterator,
			}
			assert.NotPanics(t, func() {
				p.Enqueue(tt.args.value)
			})
		})
	}
}

func TestPrioritizedNodePointerMinPriorityComparator_Equal(t *testing.T) {
	type args struct {
		value1 PrioritizedNodePointer
		value2 PrioritizedNodePointer
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Check min timestamp prioritizer equals comparator",
			args: args{
				value1: NewPrioritizedNodePointer(nil, 100),
				value2: NewPrioritizedNodePointer(nil, 100),
			},
			want: true,
		},
		{
			name: "Check min timestamp prioritizer not equals comparator",
			args: args{
				value1: NewPrioritizedNodePointer(nil, 100),
				value2: NewPrioritizedNodePointer(nil, 101),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmp := NewPrioritizedNodePointerMinPriorityComparator()
			if got := cmp.Equal(tt.args.value1, tt.args.value2); got != tt.want {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestPrioritizedNodePointerMinPriorityComparator_Less(t *testing.T) {
	type args struct {
		value1 PrioritizedNodePointer
		value2 PrioritizedNodePointer
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Check that event with lower timestamp get higher priority",
			args: args{
				value1: NewPrioritizedNodePointer(nil, 100),
				value2: NewPrioritizedNodePointer(nil, 102),
			},
			want: false,
		},
		{
			name: "Check that event with higher timestamp get lower priority",
			args: args{
				value1: NewPrioritizedNodePointer(nil, 200),
				value2: NewPrioritizedNodePointer(nil, 101),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmp := NewPrioritizedNodePointerMinPriorityComparator()
			if got := cmp.Less(tt.args.value1, tt.args.value2); got != tt.want {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestPqStorage_Dump(t *testing.T) {
	type fields struct {
		mutex       *sync.Mutex
		dataStorage *priorityqueue.PriorityQueue
		iterator    *doublylinkedlist.DoublyLinkedList
	}
	tests := []struct {
		name    string
		fields  fields
		msgs    []message.Message
		want    []message.Message
		dequeue int
	}{
		{
			name: "Check simple dump",
			fields: fields{
				mutex: &sync.Mutex{},
				dataStorage: priorityqueue.NewPriorityQueue(
					NewPrioritizedNodePointerValueList(make([]PrioritizedNodePointer, 0)),
					NewPrioritizedNodePointerMinPriorityComparator(),
				),
				iterator: doublylinkedlist.NewDoublyLinkedList(),
			},
			msgs:    []message.Message{message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
			want:    []message.Message{message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
			dequeue: 0,
		},
		{
			name: "Check dump with dequeue operation",
			fields: fields{
				mutex: &sync.Mutex{},
				dataStorage: priorityqueue.NewPriorityQueue(
					NewPrioritizedNodePointerValueList(make([]PrioritizedNodePointer, 0)),
					NewPrioritizedNodePointerMinPriorityComparator(),
				),
				iterator: doublylinkedlist.NewDoublyLinkedList(),
			},
			msgs:    []message.Message{message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
			want:    []message.Message{message.NewMessage("msg1", 2000), message.NewMessage("msg3", 2500)},
			dequeue: 1,
		},
		{
			name: "Check dump with two dequeue operations",
			fields: fields{
				mutex: &sync.Mutex{},
				dataStorage: priorityqueue.NewPriorityQueue(
					NewPrioritizedNodePointerValueList(make([]PrioritizedNodePointer, 0)),
					NewPrioritizedNodePointerMinPriorityComparator(),
				),
				iterator: doublylinkedlist.NewDoublyLinkedList(),
			},
			msgs:    []message.Message{message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
			want:    []message.Message{message.NewMessage("msg3", 2500)},
			dequeue: 2,
		},
		{
			name: "Check dump with empty data",
			fields: fields{
				mutex: &sync.Mutex{},
				dataStorage: priorityqueue.NewPriorityQueue(
					NewPrioritizedNodePointerValueList(make([]PrioritizedNodePointer, 0)),
					NewPrioritizedNodePointerMinPriorityComparator(),
				),
				iterator: doublylinkedlist.NewDoublyLinkedList(),
			},
			msgs:    []message.Message{message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
			want:    []message.Message{},
			dequeue: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:       tt.fields.mutex,
				dataStorage: tt.fields.dataStorage,
				iterator:    tt.fields.iterator,
			}
			for _, msg := range tt.msgs {
				p.Enqueue(msg)
			}
			for tt.dequeue > 0 {
				p.Dequeue()
				tt.dequeue--
			}
			if got := *p.Dump(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestPqStorage_Flush(t *testing.T) {
	type fields struct {
		mutex       *sync.Mutex
		dataStorage *priorityqueue.PriorityQueue
		iterator    *doublylinkedlist.DoublyLinkedList
	}
	tests := []struct {
		name   string
		fields fields
		msgs   []message.Message
		want   []message.Message
	}{
		{
			name: "Check flush",
			fields: fields{
				mutex: &sync.Mutex{},
				dataStorage: priorityqueue.NewPriorityQueue(
					NewPrioritizedNodePointerValueList(make([]PrioritizedNodePointer, 0)),
					NewPrioritizedNodePointerMinPriorityComparator(),
				),
				iterator: doublylinkedlist.NewDoublyLinkedList(),
			},
			msgs: []message.Message{message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
			want: []message.Message{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:       tt.fields.mutex,
				dataStorage: tt.fields.dataStorage,
				iterator:    tt.fields.iterator,
			}
			for _, msg := range tt.msgs {
				p.Enqueue(msg)
			}
			p.Flush()
			assert.Equal(t, 0, p.dataStorage.GetLength())
			assert.Equal(t, 0, p.iterator.GetLength())
			assert.Equal(t, tt.want, *p.Dump())
		})
	}
}
