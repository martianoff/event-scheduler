package storage

import (
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/go-hpds/doublylinkedlist"
	"github.com/maksimru/go-hpds/priorityqueue"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync"
	"testing"
)

func TestNewPqChannelStorage(t *testing.T) {
	tests := []struct {
		name string
		want PqChannelStorage
	}{
		{
			name: "PqChannelStorage creation",
			want: PqChannelStorage{
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
			if got := NewPqChannelStorage(); !reflect.DeepEqual(got, tt.want) {
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

func TestPqChannelStorage_CheckScheduled(t *testing.T) {
	type fields struct {
		mutex       *sync.Mutex
		dataStorage *priorityqueue.PriorityQueue
	}
	type args struct {
		nowTimestamp int
	}
	storage := NewPqChannelStorage()
	storage.Enqueue(message.NewMessage("msg1", 1000))
	storage.Enqueue(message.NewMessage("msg2", 1001))
	storage.Enqueue(message.NewMessage("msg3", 1002))
	emptyStorage := NewPqChannelStorage()
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
			p := &PqChannelStorage{
				mutex:       tt.fields.mutex,
				dataStorage: tt.fields.dataStorage,
			}
			if got := p.CheckScheduled(tt.args.nowTimestamp); got != tt.want {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestPqChannelStorage_Dequeue(t *testing.T) {
	type fields struct {
		mutex       *sync.Mutex
		dataStorage *priorityqueue.PriorityQueue
	}
	storage := NewPqChannelStorage()
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
			p := &PqChannelStorage{
				mutex:       tt.fields.mutex,
				dataStorage: tt.fields.dataStorage,
			}
			if got := p.Dequeue(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestPqChannelStorage_Enqueue(t *testing.T) {
	type fields struct {
		mutex       *sync.Mutex
		dataStorage *priorityqueue.PriorityQueue
		iterator    *doublylinkedlist.DoublyLinkedList
	}
	type args struct {
		value message.Message
	}
	storage := NewPqChannelStorage()
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
			p := &PqChannelStorage{
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

func TestPqChannelStorage_Dump(t *testing.T) {
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
			p := &PqChannelStorage{
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
			if got := p.Dump(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestPqChannelStorage_Flush(t *testing.T) {
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
			p := &PqChannelStorage{
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
			assert.Equal(t, tt.want, p.Dump())
		})
	}
}

func TestPqChannelStorage_IsEmpty(t *testing.T) {
	type fields struct {
		mutex       *sync.Mutex
		dataStorage *priorityqueue.PriorityQueue
		iterator    *doublylinkedlist.DoublyLinkedList
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
		data   []message.Message
	}{
		{
			name: "Check IsEmpty function on empty channel storage",
			fields: fields{
				mutex: &sync.Mutex{},
				dataStorage: priorityqueue.NewPriorityQueue(
					NewPrioritizedNodePointerValueList(make([]PrioritizedNodePointer, 0)),
					NewPrioritizedNodePointerMinPriorityComparator(),
				),
				iterator: doublylinkedlist.NewDoublyLinkedList(),
			},
			want: true,
			data: []message.Message{},
		},
		{
			name: "Check IsEmpty function on non empty channel storage",
			fields: fields{
				mutex: &sync.Mutex{},
				dataStorage: priorityqueue.NewPriorityQueue(
					NewPrioritizedNodePointerValueList(make([]PrioritizedNodePointer, 0)),
					NewPrioritizedNodePointerMinPriorityComparator(),
				),
				iterator: doublylinkedlist.NewDoublyLinkedList(),
			},
			want: false,
			data: []message.Message{{
				AvailableAt: 0,
				Body:        "msg",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqChannelStorage{
				mutex:       tt.fields.mutex,
				dataStorage: tt.fields.dataStorage,
				iterator:    tt.fields.iterator,
			}
			for _, m := range tt.data {
				p.Enqueue(m)
			}
			if got := p.IsEmpty(); got != tt.want {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestNewPqStorage(t *testing.T) {
	tests := []struct {
		name string
		want PqStorage
	}{
		{
			name: "Check storage constructor",
			want: PqStorage{
				mutex:    &sync.Mutex{},
				channels: make(map[string]channel.Channel),
				data:     make(map[string]PqChannelStorage),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := *NewPqStorage(); !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestPqStorage_AddChannel(t *testing.T) {
	type fields struct {
		mutex    *sync.Mutex
		channels map[string]channel.Channel
		data     map[string]PqChannelStorage
	}
	type args struct {
		c channel.Channel
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    channel.Channel
		wantErr bool
	}{
		{
			name: "Check add new channel func passing new ID",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage()},
			},
			args: args{
				c: channel.Channel{
					ID:          "ch2",
					Source:      channel.Source{},
					Destination: channel.Destination{},
				},
			},
			want: channel.Channel{
				ID:          "ch2",
				Source:      channel.Source{},
				Destination: channel.Destination{},
			},
			wantErr: false,
		},
		{
			name: "Check add new channel func without passing new ID",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage()},
			},
			args: args{
				c: channel.Channel{},
			},
			want: channel.Channel{
				Source:      channel.Source{},
				Destination: channel.Destination{},
			},
			wantErr: false,
		},
		{
			name: "Check add new channel func using existing ID",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage()},
			},
			args: args{
				c: channel.Channel{
					ID: "ch1",
				},
			},
			want:    channel.Channel{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:    tt.fields.mutex,
				channels: tt.fields.channels,
				data:     tt.fields.data,
			}
			got, err := p.AddChannel(tt.args.c)
			if tt.wantErr {
				assert.NotNil(t, err)
				return
			}
			if tt.want.ID == "" {
				assert.NotEmpty(t, got.ID)
			} else {
				if !reflect.DeepEqual(got, tt.want) {
					assert.Equal(t, tt.want, got)
				}
			}
		})
	}
}

func TestPqStorage_DeleteChannel(t *testing.T) {
	type fields struct {
		mutex    *sync.Mutex
		channels map[string]channel.Channel
		data     map[string]PqChannelStorage
	}
	type args struct {
		msgs      map[string][]message.Message
		channelID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    channel.Channel
		wantErr bool
	}{
		{
			name: "Checks removal of existing channel",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage()},
			},
			args: args{
				channelID: "ch1",
				msgs: map[string][]message.Message{
					"ch1": {
						message.Message{
							AvailableAt: 1000,
							Body:        "msg1",
						},
					},
				},
			},
			want: channel.Channel{
				ID: "ch1",
			},
			wantErr: false,
		},
		{
			name: "Checks removal of non-existing channel",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage()},
			},
			args: args{
				channelID: "ch2",
				msgs:      map[string][]message.Message{},
			},
			want:    channel.Channel{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:    tt.fields.mutex,
				channels: tt.fields.channels,
				data:     tt.fields.data,
			}
			targedData, hasTd := p.data[tt.args.channelID]
			for ch, msgs := range tt.args.msgs {
				storage := tt.fields.data[ch]
				for _, m := range msgs {
					storage.Enqueue(m)
				}
			}
			if hasTd {
				assert.Equal(t, false, targedData.IsEmpty())
			}
			got, err := p.DeleteChannel(tt.args.channelID)
			if tt.wantErr {
				assert.NotNil(t, err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
				if hasTd {
					assert.Equal(t, true, targedData.IsEmpty())
				}
			}
		})
	}
}

func TestPqStorage_Dump(t *testing.T) {
	type fields struct {
		mutex    *sync.Mutex
		channels map[string]channel.Channel
		data     map[string]PqChannelStorage
	}
	tests := []struct {
		name         string
		fields       fields
		msgs         map[string][]message.Message
		wantChannels []channel.Channel
		wantMsgs     map[string][]message.Message
	}{
		{
			name: "Check storage dump with single channel",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage()},
			},
			msgs: map[string][]message.Message{
				"ch1": {message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
			},
			wantChannels: []channel.Channel{
				{
					ID: "ch1",
				},
			},
			wantMsgs: map[string][]message.Message{
				"ch1": {message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
			},
		},
		{
			name: "Check storage dump with multiple channels",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}, "ch2": {ID: "ch2"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage(), "ch2": NewPqChannelStorage()},
			},
			msgs: map[string][]message.Message{
				"ch1": {message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
				"ch2": {message.NewMessage("msg4", 3000), message.NewMessage("msg5", 3000), message.NewMessage("msg6", 3500)},
			},
			wantChannels: []channel.Channel{
				{
					ID: "ch1",
				},
				{
					ID: "ch2",
				},
			},
			wantMsgs: map[string][]message.Message{
				"ch1": {message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
				"ch2": {message.NewMessage("msg4", 3000), message.NewMessage("msg5", 3000), message.NewMessage("msg6", 3500)},
			},
		},
		{
			name: "Check storage dump with multiple channels, swapped channels",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}, "ch2": {ID: "ch2"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage(), "ch2": NewPqChannelStorage()},
			},
			msgs: map[string][]message.Message{
				"ch1": {message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
				"ch2": {message.NewMessage("msg4", 3000), message.NewMessage("msg5", 3000), message.NewMessage("msg6", 3500)},
			},
			wantChannels: []channel.Channel{
				{
					ID: "ch1",
				},
				{
					ID: "ch2",
				},
			},
			wantMsgs: map[string][]message.Message{
				"ch2": {message.NewMessage("msg4", 3000), message.NewMessage("msg5", 3000), message.NewMessage("msg6", 3500)},
				"ch1": {message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:    tt.fields.mutex,
				channels: tt.fields.channels,
				data:     tt.fields.data,
			}
			for ch, msgs := range tt.msgs {
				storage := tt.fields.data[ch]
				for _, m := range msgs {
					storage.Enqueue(m)
				}
			}
			gotChannels, gotMsgs := p.Dump()
			assert.ElementsMatch(t, tt.wantChannels, gotChannels)
			for k, msgs := range gotMsgs {
				assert.Equal(t, tt.wantMsgs[k], msgs)
			}
			assert.Equal(t, len(tt.wantMsgs), len(gotMsgs))
		})
	}
}

func TestPqStorage_Flush(t *testing.T) {
	type fields struct {
		mutex    *sync.Mutex
		channels map[string]channel.Channel
		data     map[string]PqChannelStorage
	}
	tests := []struct {
		name         string
		fields       fields
		msgs         map[string][]message.Message
		wantMsgs     map[string][]message.Message
		wantChannels []channel.Channel
	}{
		{
			name: "Check storage flush with single channel",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage()},
			},
			msgs: map[string][]message.Message{
				"ch1": {message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
			},
			wantChannels: []channel.Channel{},
			wantMsgs:     map[string][]message.Message{},
		},
		{
			name: "Check storage flush with multiple channels",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}, "ch2": {ID: "ch2"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage(), "ch2": NewPqChannelStorage()},
			},
			msgs: map[string][]message.Message{
				"ch1": {message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
				"ch2": {message.NewMessage("msg4", 100), message.NewMessage("msg5", 200), message.NewMessage("msg6", 300)},
			},
			wantChannels: []channel.Channel{},
			wantMsgs:     map[string][]message.Message{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:    tt.fields.mutex,
				channels: tt.fields.channels,
				data:     tt.fields.data,
			}
			for ch, msgs := range tt.msgs {
				storage := tt.fields.data[ch]
				for _, m := range msgs {
					storage.Enqueue(m)
				}
			}
			p.Flush()
			gotChannels, gotMsgs := p.Dump()
			assert.Equal(t, tt.wantChannels, gotChannels)
			assert.Equal(t, tt.wantMsgs, gotMsgs)
		})
	}
}

func TestPqStorage_GetChannelStorage(t *testing.T) {
	type fields struct {
		mutex    *sync.Mutex
		channels map[string]channel.Channel
		data     map[string]PqChannelStorage
	}
	type args struct {
		channelID string
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		msgs          map[string][]message.Message
		want          []message.Message
		wantAvailable bool
	}{
		{
			name: "Check channel storage with multiple channels",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}, "ch2": {ID: "ch2"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage(), "ch2": NewPqChannelStorage()},
			},
			msgs: map[string][]message.Message{
				"ch1": {message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
				"ch2": {message.NewMessage("msg4", 3200), message.NewMessage("msg5", 3000), message.NewMessage("msg6", 3500)},
			},
			args: args{
				channelID: "ch2",
			},
			wantAvailable: true,
			want:          []message.Message{message.NewMessage("msg5", 3000), message.NewMessage("msg4", 3200), message.NewMessage("msg6", 3500)},
		},
		{
			name: "Check channel storage with multiple channels, another channel",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}, "ch2": {ID: "ch2"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage(), "ch2": NewPqChannelStorage()},
			},
			msgs: map[string][]message.Message{
				"ch1": {message.NewMessage("msg1", 2000), message.NewMessage("msg2", 1000), message.NewMessage("msg3", 2500)},
				"ch2": {message.NewMessage("msg4", 3200), message.NewMessage("msg5", 3000), message.NewMessage("msg6", 3500)},
			},
			args: args{
				channelID: "ch1",
			},
			wantAvailable: true,
			want:          []message.Message{message.NewMessage("msg2", 1000), message.NewMessage("msg1", 2000), message.NewMessage("msg3", 2500)},
		},
		{
			name: "Check channel storage with multiple channels, non existing channel",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}, "ch2": {ID: "ch2"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage(), "ch2": NewPqChannelStorage()},
			},
			msgs: map[string][]message.Message{},
			args: args{
				channelID: "ch3",
			},
			wantAvailable: false,
			want:          []message.Message{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:    tt.fields.mutex,
				channels: tt.fields.channels,
				data:     tt.fields.data,
			}
			for ch, msgs := range tt.msgs {
				storage := tt.fields.data[ch]
				for _, m := range msgs {
					storage.Enqueue(m)
				}
			}
			s, available := p.GetChannelStorage(tt.args.channelID)
			assert.Equal(t, tt.wantAvailable, available)
			if !available {
				return
			}
			got := make([]message.Message, s.dataStorage.GetLength())
			i := 0
			for s.IsEmpty() != true {
				got[i] = s.Dequeue()
				i++
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPqStorage_GetChannels(t *testing.T) {
	type fields struct {
		mutex    *sync.Mutex
		channels map[string]channel.Channel
		data     map[string]PqChannelStorage
	}
	tests := []struct {
		name   string
		fields fields
		want   []channel.Channel
	}{
		{
			name: "Check get channels",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}, "ch2": {ID: "ch2"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage(), "ch2": NewPqChannelStorage()},
			},
			want: []channel.Channel{{ID: "ch1"}, {ID: "ch2"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:    tt.fields.mutex,
				channels: tt.fields.channels,
				data:     tt.fields.data,
			}
			if got := p.GetChannels(); !reflect.DeepEqual(got, tt.want) {
				assert.ElementsMatch(t, tt.want, got)
			}
		})
	}
}

func TestPqStorage_GetChannel(t *testing.T) {
	type fields struct {
		mutex    *sync.Mutex
		channels map[string]channel.Channel
		data     map[string]PqChannelStorage
	}
	tests := []struct {
		name    string
		fields  fields
		want    channel.Channel
		wantErr bool
	}{
		{
			name: "Check get channel",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {ID: "ch1"}},
				data:     map[string]PqChannelStorage{"ch1": NewPqChannelStorage()},
			},
			want:    channel.Channel{ID: "ch1"},
			wantErr: false,
		},
		{
			name: "Check get missing channel",
			fields: fields{
				mutex:    &sync.Mutex{},
				channels: map[string]channel.Channel{"ch2": {ID: "ch2"}},
				data:     map[string]PqChannelStorage{"ch2": NewPqChannelStorage()},
			},
			want:    channel.Channel{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:    tt.fields.mutex,
				channels: tt.fields.channels,
				data:     tt.fields.data,
			}
			got, err := p.GetChannel("ch1")
			assert.Equal(t, tt.want, got)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

		})
	}
}

func TestPqStorage_UpdateChannel(t *testing.T) {
	type fields struct {
		mutex    *sync.Mutex
		channels map[string]channel.Channel
		data     map[string]PqChannelStorage
	}
	type args struct {
		channelID string
		c         channel.Channel
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    channel.Channel
		wantErr bool
	}{
		{
			name: "Check existing channel update",
			fields: fields{
				mutex: &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {
					ID: "ch1",
					Source: channel.Source{
						Driver: "test",
					},
				}},
				data: map[string]PqChannelStorage{"ch1": NewPqChannelStorage()},
			},
			args: args{
				channelID: "ch1",
				c: channel.Channel{
					ID: "ch1",
					Source: channel.Source{
						Driver: "pubsub",
					},
				},
			},
			want: channel.Channel{
				ID: "ch1",
				Source: channel.Source{
					Driver: "pubsub",
				},
			},
			wantErr: false,
		},
		{
			name: "Check channel id is immutable during update",
			fields: fields{
				mutex: &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {
					ID: "ch1",
					Source: channel.Source{
						Driver: "test",
					},
				}},
				data: map[string]PqChannelStorage{"ch1": NewPqChannelStorage()},
			},
			args: args{
				channelID: "ch1",
				c: channel.Channel{
					ID: "ch2",
					Source: channel.Source{
						Driver: "pubsub",
					},
				},
			},
			want: channel.Channel{
				ID: "ch1",
				Source: channel.Source{
					Driver: "pubsub",
				},
			},
			wantErr: false,
		},
		{
			name: "Check update of non existing channel",
			fields: fields{
				mutex: &sync.Mutex{},
				channels: map[string]channel.Channel{"ch1": {
					ID: "ch1",
					Source: channel.Source{
						Driver: "test",
					},
				}},
				data: map[string]PqChannelStorage{"ch1": NewPqChannelStorage()},
			},
			args: args{
				channelID: "ch2",
				c: channel.Channel{
					ID: "ch2",
					Source: channel.Source{
						Driver: "pubsub",
					},
				},
			},
			want:    channel.Channel{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PqStorage{
				mutex:    tt.fields.mutex,
				channels: tt.fields.channels,
				data:     tt.fields.data,
			}
			got, err := p.UpdateChannel(tt.args.channelID, tt.args.c)
			if tt.wantErr {
				assert.NotNil(t, err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
			}
			assert.Equal(t, tt.args.channelID, got.ID)
		})
	}
}
