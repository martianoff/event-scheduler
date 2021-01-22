package test

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/message"
)

type Publisher struct {
}

func (p *Publisher) Boot(ctx context.Context, config config.Config, outboundPool *goconcurrentqueue.FIFO) error {
	return nil
}

func (p *Publisher) Push(msg message.Message) error {
	return nil
}

func (p Publisher) Dispatch() error {
	return nil
}
