package test

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
)

type Listener struct {
}

func (l *Listener) Boot(ctx context.Context, config config.Config, inboundPool *goconcurrentqueue.FIFO) error {
	return nil
}

func (l *Listener) Listen() error {
	return nil
}
