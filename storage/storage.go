package storage

import (
	"github.com/maksimru/go-hpds/priorityqueue"
	"github.com/maksimru/go-hpds/utils/comparator"
	"sync"
)

type StringMinPriorityComparator struct{ comparator.AbstractComparator }

func NewStringMinPriorityComparator() *StringMinPriorityComparator {
	cmp := StringMinPriorityComparator{comparator.AbstractComparator{}}
	cmp.AbstractComparator.Comparator = cmp
	return &cmp
}

func (cmp StringMinPriorityComparator) Less(value1 interface{}, value2 interface{}) bool {
	return value2.(priorityqueue.StringPrioritizedValue).GetPriority() < value1.(priorityqueue.StringPrioritizedValue).GetPriority()
}

func (cmp StringMinPriorityComparator) Equal(value1 interface{}, value2 interface{}) bool {
	return value1.(priorityqueue.StringPrioritizedValue).GetPriority() == value2.(priorityqueue.StringPrioritizedValue).GetPriority()
}

type PqStorage struct {
	mutex       *sync.Mutex
	dataStorage *priorityqueue.PriorityQueue
}

func NewPqStorage() *PqStorage {
	return &PqStorage{
		mutex: &sync.Mutex{},
		dataStorage: priorityqueue.NewPriorityQueue(
			priorityqueue.NewStringPrioritizedValueList(make([]priorityqueue.StringPrioritizedValue, 0)),
			NewStringMinPriorityComparator(),
		),
	}
}

func (p *PqStorage) Dequeue() priorityqueue.StringPrioritizedValue {
	p.mutex.Lock()
	value := p.dataStorage.Dequeue()
	p.mutex.Unlock()
	return value.(priorityqueue.StringPrioritizedValue)
}

func (p *PqStorage) Enqueue(value priorityqueue.PrioritizedValue) {
	p.mutex.Lock()
	p.dataStorage.Enqueue(value)
	p.mutex.Unlock()
}

func (p *PqStorage) CheckScheduled(nowTimestamp int) bool {
	if p.dataStorage.Top() == nil {
		return false
	}
	earliestScheduledTimestamp := p.dataStorage.Top().GetPriority()
	match := earliestScheduledTimestamp <= nowTimestamp
	return match
}
