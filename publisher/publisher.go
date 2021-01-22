package publisher

import (
	"context"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/maksimru/event-scheduler/config"
	"github.com/maksimru/event-scheduler/message"
)

type Publisher interface {
	Boot(context.Context, config.Config, *goconcurrentqueue.FIFO) error
	Push(message.Message) error
	Dispatch() error
}