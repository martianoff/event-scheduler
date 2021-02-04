package test

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/hashicorp/raft"
	"github.com/maksimru/event-scheduler/config"
)

type Listener struct {
}

func (l *Listener) Boot(context.Context, config.Config, *goconcurrentqueue.FIFO, *raft.Raft) error {
	return nil
}

func (l *Listener) Listen() error {
	return nil
}
