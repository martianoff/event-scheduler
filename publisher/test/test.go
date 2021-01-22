package test

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/message"
)

type Publisher struct {
}

func (p *Publisher) Boot(context.Context, config.Config, *goconcurrentqueue.FIFO) error {
	return nil
}

func (p *Publisher) Push(message.Message) error {
	return nil
}

func (p Publisher) Dispatch() error {
	return nil
}
