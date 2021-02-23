package test

import (
	"context"
	"errors"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/channel"
	"github.com/maksimru/event-scheduler/message"
)

type Publisher struct {
	broken bool
}

func (p *Publisher) Boot(context.Context, channel.Channel, *goconcurrentqueue.FIFO) error {
	p.broken = false
	return nil
}

func (p *Publisher) Push(message.Message) error {
	return nil
}

func (p *Publisher) WrongConfig() *Publisher {
	p.broken = true
	return p
}

func (p *Publisher) CorrectConfig() *Publisher {
	p.broken = false
	return p
}

func (p Publisher) Dispatch() error {
	if p.broken {
		return errors.New("publisher dispatch exception")
	}
	return nil
}
