package test

import (
	"errors"
	"github.com/maksimru/event-scheduler/message"
)

type Publisher struct {
	broken     bool
	dispatched []message.Message
}

func NewTestPublisher() *Publisher {
	return &Publisher{}
}

func (p *Publisher) WrongConfig() *Publisher {
	p.broken = true
	return p
}

func (p *Publisher) CorrectConfig() *Publisher {
	p.broken = false
	return p
}

func (p *Publisher) Dispatch(msg message.Message) error {
	if p.broken {
		return errors.New("publisher dispatch exception")
	}
	if p.dispatched == nil {
		p.dispatched = make([]message.Message, 0)
	}
	p.dispatched = append(p.dispatched, msg)
	return nil
}

func (p *Publisher) GetDispatched() []message.Message {
	return p.dispatched
}

func (p *Publisher) Close() error {
	return nil
}
