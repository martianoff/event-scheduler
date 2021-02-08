package test

import (
	"context"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/prioritizer"
	log "github.com/sirupsen/logrus"
	"time"
)

type Listener struct {
	context  context.Context
	stopFunc context.CancelFunc
}

func (l *Listener) Boot(ctx context.Context, config config.Config, prioritizer *prioritizer.Prioritizer) error {
	l.context = ctx
	return nil
}

func (l *Listener) Listen() error {
	ctx, cancelListener := context.WithCancel(l.context)
	defer cancelListener()
	l.stopFunc = cancelListener
listenerloop:
	for {
		select {
		case <-ctx.Done():
			break listenerloop
		default:
			time.Sleep(time.Millisecond * 500)
		}
	}
	log.Info("listener stopped")
	return nil
}

func (l *Listener) Stop() error {
	if l.stopFunc != nil {
		log.Info("listener stop called")
		l.stopFunc()
	}
	return nil
}
