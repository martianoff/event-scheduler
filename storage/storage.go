package storage

import (
	"errors"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/message"
	"github.com/maksimru/go-hpds/doublylinkedlist"
	"github.com/maksimru/go-hpds/priorityqueue"
	"github.com/maksimru/go-hpds/utils/arraylist"
	"github.com/maksimru/go-hpds/utils/comparator"
	"github.com/satori/go.uuid"
	"sync"
)

var (
	ErrChannelNotFound = errors.New("channel is not found")
)

type PrioritizedNodePointer struct {
	value    *doublylinkedlist.Node
	priority int
}

func NewPrioritizedNodePointer(value *doublylinkedlist.Node, priority int) PrioritizedNodePointer {
	return PrioritizedNodePointer{value: value, priority: priority}
}

func (v PrioritizedNodePointer) GetValue() interface{} {
	return v.value
}

func (v PrioritizedNodePointer) GetPriority() int {
	return v.priority
}

type PrioritizedNodePointerMinPriorityComparator struct{ comparator.AbstractComparator }

func NewPrioritizedNodePointerMinPriorityComparator() *PrioritizedNodePointerMinPriorityComparator {
	cmp := PrioritizedNodePointerMinPriorityComparator{comparator.AbstractComparator{}}
	cmp.AbstractComparator.Comparator = cmp
	return &cmp
}

func (cmp PrioritizedNodePointerMinPriorityComparator) Less(value1 interface{}, value2 interface{}) bool {
	return value2.(PrioritizedNodePointer).GetPriority() < value1.(PrioritizedNodePointer).GetPriority()
}

func (cmp PrioritizedNodePointerMinPriorityComparator) Equal(value1 interface{}, value2 interface{}) bool {
	return value1.(PrioritizedNodePointer).GetPriority() == value2.(PrioritizedNodePointer).GetPriority()
}

type PrioritizedNodePointerValueList struct {
	data []PrioritizedNodePointer
	arraylist.AbstractArrayList
}

func NewPrioritizedNodePointerValueList(arr []PrioritizedNodePointer) *PrioritizedNodePointerValueList {
	list := PrioritizedNodePointerValueList{arr, arraylist.AbstractArrayList{}}
	list.AbstractArrayList.ArrayList = &list
	return &list
}

func (list *PrioritizedNodePointerValueList) Get(index int) interface{} {
	return list.data[index]
}

func (list *PrioritizedNodePointerValueList) Set(index int, value interface{}) {
	list.data[index] = value.(PrioritizedNodePointer)
}

func (list *PrioritizedNodePointerValueList) GetLength() int {
	return len(list.data)
}

func (list *PrioritizedNodePointerValueList) RemoveLast() interface{} {
	valueForRemoval := list.data[list.GetLength()-1]
	list.data = list.data[0 : list.GetLength()-1]
	return valueForRemoval
}

func (list *PrioritizedNodePointerValueList) Add(value interface{}) {
	list.data = append(list.data, value.(PrioritizedNodePointer))
}

func (list *PrioritizedNodePointerValueList) Clean() {
	list.data = nil
}

type PqStorage struct {
	mutex    *sync.Mutex
	channels map[string]channel.Channel
	data     map[string]PqChannelStorage
}

func NewPqStorage() *PqStorage {
	return &PqStorage{
		mutex:    &sync.Mutex{},
		channels: make(map[string]channel.Channel),
		data:     make(map[string]PqChannelStorage),
	}
}

func (p *PqStorage) Flush() {
	p.mutex.Lock()
	p.channels = make(map[string]channel.Channel)
	p.data = make(map[string]PqChannelStorage)
	p.mutex.Unlock()
}

func (p *PqStorage) Dump() ([]channel.Channel, map[string][]message.Message) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	m := make(map[string][]message.Message, len(p.channels))
	channels := make([]channel.Channel, len(p.channels))
	i := 0
	for _, c := range p.channels {
		channels[i] = c
		i++
		pq := p.data[c.ID]
		m[c.ID] = pq.Dump()
	}

	return channels, m
}

func (p *PqStorage) GetChannel(channelID string) (channel.Channel, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	c, has := p.channels[channelID]
	if !has {
		return channel.Channel{}, ErrChannelNotFound
	}

	return c, nil
}

func (p *PqStorage) GetChannels() []channel.Channel {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	channels := make([]channel.Channel, len(p.channels))
	i := 0
	for _, v := range p.channels {
		channels[i] = v
		i++
	}

	return channels
}

func (p *PqStorage) AddChannel(c channel.Channel) (channel.Channel, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	uid := c.ID
	if uid == "" {
		uid = uuid.NewV4().String()
		c.ID = uid
	} else {
		_, has := p.channels[uid]
		if has {
			return channel.Channel{}, errors.New("channel is already exists")
		}
	}
	p.channels[uid] = c
	// automatically create storage for the new channel
	if _, has := p.data[uid]; !has {
		p.data[uid] = NewPqChannelStorage()
	}
	return p.channels[uid], nil
}

func (p *PqStorage) UpdateChannel(channelID string, c channel.Channel) (channel.Channel, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	_, has := p.channels[channelID]
	if !has {
		return channel.Channel{}, ErrChannelNotFound
	}

	// do not let override channel id
	c.ID = channelID
	p.channels[channelID] = c
	return p.channels[channelID], nil
}

func (p *PqStorage) DeleteChannel(channelID string) (channel.Channel, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	c, has := p.channels[channelID]
	if !has {
		return channel.Channel{}, ErrChannelNotFound
	}
	delete(p.channels, channelID)
	s, has := p.GetChannelStorage(channelID)
	if has {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		delete(p.data, channelID)
	}
	return c, nil
}

func (p *PqStorage) GetChannelStorage(channelID string) (PqChannelStorage, bool) {
	storage, has := p.data[channelID]
	return storage, has
}

type PqChannelStorage struct {
	mutex       *sync.Mutex
	dataStorage *priorityqueue.PriorityQueue
	iterator    *doublylinkedlist.DoublyLinkedList
}

func NewPqChannelStorage() PqChannelStorage {
	return PqChannelStorage{
		mutex: &sync.Mutex{},
		dataStorage: priorityqueue.NewPriorityQueue(
			NewPrioritizedNodePointerValueList(make([]PrioritizedNodePointer, 0)),
			NewPrioritizedNodePointerMinPriorityComparator(),
		),
		iterator: doublylinkedlist.NewDoublyLinkedList(),
	}
}

func (p *PqChannelStorage) Dequeue() message.Message {
	p.mutex.Lock()
	priorityData := p.dataStorage.Dequeue().(PrioritizedNodePointer)
	msgPtr := priorityData.GetValue().(*doublylinkedlist.Node)
	msgData := msgPtr.GetValue().(message.Message)
	msgPtr.Remove()
	p.mutex.Unlock()
	return msgData
}

func (p *PqChannelStorage) Enqueue(value message.Message) {
	p.mutex.Lock()
	node := p.iterator.Append(value)
	p.dataStorage.Enqueue(NewPrioritizedNodePointer(node, value.GetAvailableAt()))
	p.mutex.Unlock()
}

func (p *PqChannelStorage) IsEmpty() bool {
	return p.dataStorage.IsEmpty()
}

func (p *PqChannelStorage) CheckScheduled(nowTimestamp int) bool {
	p.mutex.Lock()
	top := p.dataStorage.Top()
	p.mutex.Unlock()
	if top == nil {
		return false
	}
	earliestScheduledTimestamp := top.GetPriority()
	match := earliestScheduledTimestamp <= nowTimestamp
	return match
}

func (p *PqChannelStorage) Flush() {
	p.mutex.Lock()
	p.iterator.Purge()
	p.dataStorage.Clean()
	p.mutex.Unlock()
}

func (p *PqChannelStorage) Dump() []message.Message {
	p.mutex.Lock()
	clone := make([]message.Message, p.dataStorage.GetLength())
	node := p.iterator.GetHead()
	i := 0
	for node != nil {
		clone[i] = node.GetValue().(message.Message)
		node = node.GetNext()
		i++
	}
	p.mutex.Unlock()
	return clone
}
