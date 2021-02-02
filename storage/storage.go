package storage

import (
	"github.com/maksimru/go-hpds/doublylinkedlist"
	"github.com/maksimru/go-hpds/priorityqueue"
	"github.com/maksimru/go-hpds/utils/arraylist"
	"github.com/maksimru/go-hpds/utils/comparator"
	"sync"
)

type PrioritizedNodePointer struct {
	value    *doublylinkedlist.Node
	priority int
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
	mutex       *sync.Mutex
	dataStorage *priorityqueue.PriorityQueue
	iterator    *doublylinkedlist.DoublyLinkedList
}

func NewPqStorage() *PqStorage {
	return &PqStorage{
		mutex: &sync.Mutex{},
		dataStorage: priorityqueue.NewPriorityQueue(
			NewPrioritizedNodePointerValueList(make([]PrioritizedNodePointer, 0)),
			NewPrioritizedNodePointerMinPriorityComparator(),
		),
		iterator: doublylinkedlist.NewDoublyLinkedList(),
	}
}

func (p *PqStorage) Dequeue() priorityqueue.StringPrioritizedValue {
	p.mutex.Lock()
	priorityData := p.dataStorage.Dequeue().(PrioritizedNodePointer)
	msgPtr := priorityData.GetValue().(*doublylinkedlist.Node)
	msgData := msgPtr.GetValue().(priorityqueue.StringPrioritizedValue)
	msgPtr.Remove()
	p.mutex.Unlock()
	return msgData
}

func (p *PqStorage) Enqueue(value priorityqueue.PrioritizedValue) {
	p.mutex.Lock()
	node := p.iterator.Append(value)
	p.dataStorage.Enqueue(PrioritizedNodePointer{value: node, priority: value.GetPriority()})
	p.mutex.Unlock()
}

func (p *PqStorage) IsEmpty() bool {
	return p.dataStorage.IsEmpty()
}

func (p *PqStorage) CheckScheduled(nowTimestamp int) bool {
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

func (p *PqStorage) Flush() {
	p.mutex.Lock()
	p.iterator.Purge()
	p.dataStorage.Clean()
	p.mutex.Unlock()
}

func (p *PqStorage) Dump() *[]priorityqueue.StringPrioritizedValue {
	p.mutex.Lock()
	clone := make([]priorityqueue.StringPrioritizedValue, p.dataStorage.GetLength())
	node := p.iterator.GetHead()
	i := 0
	for node != nil {
		clone[i] = node.GetValue().(priorityqueue.StringPrioritizedValue)
		node = node.GetNext()
		i++
	}
	p.mutex.Unlock()
	return &clone
}
